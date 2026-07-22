# Intermediate 1 — Producer Internals

Elephant Scale

---

## Agenda

- Serialization; the partitioner (key-based vs. round-robin)
- Batching, `linger.ms`, `batch.size`, compression
- Acknowledgements: `acks=0/1/all` and durability trade-offs
- Idempotent producers — exactly-once at the producer

---

## What a Producer Actually Does

`producer.produce(...)` looks instant, but a lot happens between your call and a
durable record on a broker.

```
your code
  │  produce(topic, key, value)
  ▼
serialize key & value  →  bytes
  ▼
partitioner  →  choose partition
  ▼
accumulate in a per-partition batch (in memory)
  ▼
sender thread  →  send batch to the partition leader
  ▼
broker acknowledges  →  delivery callback fires
```

- The call is **asynchronous** — it returns before the broker has the record
- Understanding these stages is how you tune for **throughput, latency, and durability**

Notes: The whole module is a walk down this pipeline. Anchor every config we discuss to a stage on this diagram.

---

## Stage 1: Serialization

Kafka stores **bytes**. Your keys and values must be turned into bytes on the way in.

- The producer needs a **serializer** for the key and one for the value
- Common choices: strings, JSON, and — with the Schema Registry — **Avro / Protobuf** (Module 8)
- Consumers must use a **matching deserializer**, or they read garbage

```python
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})

producer.produce(
    topic='orders',
    key='user-1',                              # str -> bytes (utf-8)
    value=json.dumps({'id': 1, 'amt': 9.99}).encode('utf-8'),
)
producer.flush()
```

Notes: In `confluent-kafka` you typically hand it `bytes` (or `str`, which it encodes as UTF-8). JSON here is just "dict -> string -> bytes." Module 8 replaces this hand-rolled JSON with schema-backed serializers.

---

## Stage 2: The Partitioner

The producer must pick **which partition** each record goes to. That's the
**partitioner**.

- **Key present** → `hash(key) % partitions` → same key always → same partition (**ordering**)
- **No key** → spread across partitions for even load (sticky batching, round-robin-ish)
- You can supply a **custom partitioner** for special routing, but rarely need to

```
key "user-1" ─hash─► P2   (every user-1 record, in order)
key "user-2" ─hash─► P0
key = None   ─────► P0,P1,P2  (balanced)
```

**Design rule:** choose a key when you need per-entity ordering; go keyless for
maximum spread. This is the single most important producer decision.

---

## Ordering: The Guarantee and Its Limits

Kafka guarantees order **within a partition**, never across partitions.

- All records with the same key share a partition → ordered relative to each other
- Records with different keys may be in different partitions → **no** cross-partition order
- Pick your key to match your ordering requirement (per user, per order, per device)

```
partition 0:  user-2:A  user-2:B  user-2:C     ← ordered
partition 2:  user-1:X  user-1:Y               ← ordered
across P0 and P2: no ordering relationship
```

Notes: A frequent production bug: needing per-customer order but producing keyless "for balance," then being surprised events interleave. The key *is* the ordering contract.

---

## Stage 3: Batching

The producer does **not** send one record per request. It **batches** records per
partition, which is where its throughput comes from.

Two knobs decide when a batch is sent:

- **`batch.size`** — max bytes per batch (per partition). Full batch → send now.
- **`linger.ms`** — how long to *wait* for more records before sending a partial batch.

```
linger.ms = 0    → send as soon as possible (lowest latency, smaller batches)
linger.ms = 10   → wait up to 10ms to fill the batch (higher throughput)
```

**The trade-off:** a little `linger.ms` dramatically improves throughput under load,
at the cost of a few ms of latency. Default `linger.ms` is small; raise it to batch harder.

---

## Stage 3b: Compression

Batches can be **compressed** before they hit the network and disk — a big win because
Kafka compresses the whole batch, not record-by-record.

- `compression.type`: `none`, `gzip`, `snappy`, `lz4`, `zstd`
- **`zstd`** and **`lz4`** are the usual picks — strong ratio, low CPU
- Compression pairs with batching: **bigger batches compress better**
- The broker stores it compressed; the consumer decompresses — end to end

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 10,
    'batch.size': 65536,
    'compression.type': 'zstd',
})
```

Notes: Compression reduces network, disk, *and* replication traffic. On JSON especially the ratio is large. It costs producer CPU, but zstd/lz4 make that cheap.

---

## Stage 4: Acknowledgements (`acks`)

When is a write "done"? That's **`acks`** — the durability dial.

| `acks` | Waits for | Durability | Risk |
|---|---|---|---|
| `0` | nothing (fire-and-forget) | lowest | lose records silently |
| `1` | leader only | medium | lose records if leader dies before replication |
| `all` | leader **+ all in-sync replicas** | highest | none once acknowledged |

```
acks=0    produce ──► (don't wait)             fastest, least safe
acks=1    produce ──► leader wrote ──► ack      middle ground
acks=all  produce ──► leader + ISR wrote ──► ack   safe, slightly slower
```

**Default and recommendation for real data: `acks=all`.**

---

## `acks=all` Needs a Partner: `min.insync.replicas`

`acks=all` means "all *in-sync* replicas" — but if the ISR has shrunk to just the
leader, "all" is one. The topic/broker setting **`min.insync.replicas`** sets the floor.

- `min.insync.replicas=2` with RF 3 → a write needs **2** in-sync replicas to succeed
- If too few replicas are in sync, the producer gets an error instead of a false "durable"
- Together: `acks=all` + `min.insync.replicas=2` = "acknowledged means safely on ≥2 brokers"

Notes: This is the durability contract. We only introduce it here; Module 7 (Reliability) makes it hands-on. The point for producers: `acks=all` alone isn't enough without the ISR floor.

---

## Retries and Ordering

If a send fails (leader moved, transient network), the producer **retries** — which is
usually what you want, but it interacts with ordering.

- Retries are on by default; a retried batch could, in theory, arrive **after** a later batch
- **`max.in.flight.requests.per.connection`** controls how many unacked batches are in flight
- Historically you capped this to preserve order — but **idempotence** (next slides) solves it
  cleanly

**Takeaway:** don't disable retries to "keep order." Turn on idempotence instead.

---

## The Duplicate Problem

A subtle failure: the broker **writes** the record, then the **ack is lost** on the way
back. The producer thinks it failed and **retries** → the record is written **twice**.

```
producer ──► broker writes record ✔
producer ◄╳─ ack lost
producer ──► retry ──► broker writes record AGAIN  →  duplicate
```

- With plain `acks=all` + retries, you get **at-least-once**: no loss, but possible duplicates
- For many pipelines duplicates are unacceptable (double charges, double counts)

This is exactly what the **idempotent producer** fixes.

---

## Idempotent Producer

Turn on idempotence and the broker will **de-duplicate** the producer's retries
automatically.

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,     # exactly-once *at the producer*
    'acks': 'all',                  # required; set implicitly
})
```

How it works, briefly:

- The producer gets a **Producer ID (PID)** and attaches a **sequence number** per partition
- The broker tracks the last sequence it accepted per (PID, partition)
- A **retried** record has the same sequence → the broker **discards the duplicate**

Result: **exactly-once delivery from producer to broker**, even across retries. Cheap
enough to be a default.

---

## What Idempotence Does and Doesn't Cover

Be precise about the guarantee — it's a common interview and production trap.

- ✅ De-dupes **this producer's retries** to a partition → no duplicates from resend
- ✅ Preserves **ordering** even with retries and multiple in-flight batches
- ❌ Does **not** de-dupe if *your application* calls `produce()` twice for the same event
- ❌ Does **not** span **multiple partitions or a consume-process-produce loop** — that needs
  **transactions** (Module 7 / Delivery Semantics)

**Idempotence = exactly-once *to the broker*. Transactions = exactly-once *across* a
processing step.**

Notes: Draw the boundary clearly. Idempotence is a producer-local guarantee. End-to-end exactly-once (read-process-write) is the transactions story, which is the next big module.

---

## A Tuned Producer, End to End

Putting the pipeline together — a sensible production baseline:

```python
from confluent_kafka import Producer
import json

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,     # no duplicate retries, order preserved
    'acks': 'all',                  # durable once acknowledged
    'linger.ms': 10,                # batch a little for throughput
    'compression.type': 'zstd',     # cheap, strong compression
})

def delivery_report(err, msg):
    if err:
        print(f'FAILED: {err}')
    else:
        print(f'ok → {msg.topic()}[{msg.partition()}]@{msg.offset()}')

producer.produce('orders', key='user-1',
                 value=json.dumps({'id': 1}).encode(),
                 callback=delivery_report)
producer.flush()      # block until outstanding deliveries complete
```

Notes: `flush()` is essential in short scripts — without it the process can exit before the async sender delivers. The `delivery_report` callback is how you learn the final partition/offset or the error.

---

## Summary

- The producer pipeline: **serialize → partition → batch → send → ack → callback**
- **Keys** drive partitioning and per-partition **ordering** — the key *is* the ordering contract
- **`linger.ms` / `batch.size` / compression** trade a little latency for large throughput gains
- **`acks=all`** (with `min.insync.replicas`) is the durability baseline for real data
- Plain retries give **at-least-once** (possible duplicates); **`enable.idempotence`** upgrades
  that to **exactly-once to the broker** and preserves order
- End-to-end exactly-once across a processing step needs **transactions** — coming up
