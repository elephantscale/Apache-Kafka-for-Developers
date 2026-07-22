# Lab 2 — Producer Internals & Tuning

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 1 — Producer Internals
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

## Objectives

By the end of this lab you will be able to:

- Write a Python producer with `confluent-kafka` and confirm delivery via callbacks
- Control partitioning with message keys and observe per-key ordering
- Tune batching (`linger.ms`, `batch.size`) and compression, and measure the effect
- Choose an `acks` level and reason about its durability trade-off
- Enable the idempotent producer and understand what it does — and doesn't — guarantee

## Prerequisites

- The lab environment from [`labs/SETUP.md`](../SETUP.md); the core cluster running
  (`docker compose up -d`, all three brokers healthy)
- The Python venv active with `confluent-kafka` installed:
  ```bash
  source .venv/bin/activate
  python -c "import confluent_kafka; print(confluent_kafka.__version__)"
  ```
- Lab 01 completed (you can create topics and read consumer lag)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no
> ZooKeeper, no Kubernetes). Your code runs on the **host** in Python and connects to
> `localhost:9092`. Kafka CLI tools run inside the brokers via `docker exec kafka-1 …`.
> Save each Python file in your working directory and run it with the venv active.

### Create the lab topic

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab02-orders --partitions 3 --replication-factor 3
```

---

## Exercise 1 — A First Producer with Delivery Reports

> **What this shows:** `produce()` is asynchronous — it queues a record and returns
> immediately. You only learn the outcome (final partition/offset, or an error) from the
> **delivery callback**, and only after `flush()`/`poll()` lets the background sender run.

### 1.1 Write the producer

```python
# save as producer_basic.py
import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'FAILED: {err}')
    else:
        print(f'ok  {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}')

for i in range(10):
    event = {'order_id': i, 'status': 'PLACED'}
    producer.produce(
        topic='lab02-orders',
        key=f'user-{i % 3}',                    # 3 distinct keys
        value=json.dumps(event).encode('utf-8'),
        callback=delivery_report,
    )
    producer.poll(0)        # give the sender thread a chance to fire callbacks

producer.flush()            # block until all deliveries complete
print('done')
```

### 1.2 Run it

```bash
python producer_basic.py
```

You'll see ten `ok …[partition] @ offset …` lines. Note that keys `user-0/1/2` map to
specific partitions.

> **If a sharp student asks:** why `producer.poll(0)`? The client delivers callbacks from
> its background thread only when you call `poll()` or `flush()`. Without it, all ten
> callbacks would fire at the end during `flush()` — fine here, but in a long-running
> producer you call `poll(0)` in the loop so callbacks and errors surface promptly.

### 1.3 Confirm from the CLI

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab02-orders --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true
```

---

## Exercise 2 — Keys, Partitioning, and Ordering

> **What this shows:** the key determines the partition (`hash(key) % partitions`), and a
> key is the *only* way to guarantee ordering for an entity. Keyless records spread across
> partitions and have no cross-partition order.

### 2.1 Prove same-key → same-partition

```python
# save as producer_keys.py
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
seen = {}

def report(err, msg):
    if err is None:
        seen.setdefault(msg.key().decode(), set()).add(msg.partition())

for i in range(30):
    key = f'user-{i % 3}'
    producer.produce('lab02-orders', key=key, value=f'event-{i}', callback=report)
producer.flush()

for key, partitions in sorted(seen.items()):
    print(f'{key} -> partition(s) {sorted(partitions)}')
```

```bash
python producer_keys.py
```

Each key prints **exactly one** partition — no key ever spans partitions.

### 2.2 Now go keyless

Change the `produce(...)` line to drop the key:

```python
    producer.produce('lab02-orders', value=f'event-{i}', callback=report)
```

Adjust `report` to bucket by value instead of key, re-run, and observe that keyless
records are **spread across all three partitions**.

> **If a sharp student asks:** two different keys landed on the same partition — is that a
> bug? No. Keys are hashed into 3 buckets, so distinct keys can collide onto one partition.
> The guarantee is one-directional: one key never *splits* across partitions.

---

## Exercise 3 — Batching, Linger, and Compression

> **What this shows:** batching is where producer throughput comes from. `linger.ms` lets
> the producer wait a few milliseconds to fill larger batches; compression then shrinks
> those batches over the network and on disk. You'll measure the difference.

### 3.1 A throughput harness

```python
# save as producer_throughput.py
import sys, time, json
from confluent_kafka import Producer

# Pass config as CLI args: linger_ms compression  (e.g. "0 none" or "20 zstd")
linger = int(sys.argv[1]) if len(sys.argv) > 1 else 0
compression = sys.argv[2] if len(sys.argv) > 2 else 'none'

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': linger,
    'batch.size': 65536,
    'compression.type': compression,
    'acks': 'all',
})

N = 100_000
payload = json.dumps({'data': 'x' * 200}).encode()   # ~200-byte records

start = time.time()
for i in range(N):
    producer.produce('lab02-orders', key=f'k-{i % 1000}', value=payload)
    if i % 10_000 == 0:
        producer.poll(0)
producer.flush()
elapsed = time.time() - start

print(f'linger.ms={linger:<3} compression={compression:<6} '
      f'{N} records in {elapsed:.2f}s  →  {N/elapsed:,.0f} rec/s')
```

### 3.2 Compare configurations

```bash
python producer_throughput.py 0  none
python producer_throughput.py 20 none
python producer_throughput.py 20 zstd
```

Compare the `rec/s`. Typically `linger.ms=20` beats `0`, and `zstd` on top adds more —
larger, compressed batches make far better use of the network.

> **If a sharp student asks:** doesn't `linger.ms=20` add 20ms of latency to every record?
> At most 20ms, and only when the batch isn't already full. Under load, batches fill before
> the timer expires, so you get the throughput win with negligible added latency. Under a
> trickle, you pay up to 20ms — usually a fine trade.

---

## Exercise 4 — Acks and Durability

> **What this shows:** `acks` is the durability dial. You'll see that all three levels
> "work" against a healthy cluster — the difference only appears under failure — so the
> choice is about what you're willing to lose, not about whether it runs.

### 4.1 Time the acks levels

```python
# save as producer_acks.py
import sys, time
from confluent_kafka import Producer

acks = sys.argv[1] if len(sys.argv) > 1 else 'all'   # 0 | 1 | all
producer = Producer({'bootstrap.servers': 'localhost:9092', 'acks': acks})

N = 50_000
start = time.time()
for i in range(N):
    producer.produce('lab02-orders', key=f'k-{i}', value=f'v-{i}')
    if i % 10_000 == 0:
        producer.poll(0)
producer.flush()
print(f'acks={acks:<3} {N} records in {time.time()-start:.2f}s')
```

```bash
python producer_acks.py 0
python producer_acks.py 1
python producer_acks.py all
```

`acks=0` is fastest and `acks=all` slowest, but on a healthy 3-broker cluster the gap is
small — and only `acks=all` guarantees no loss if a broker fails mid-write.

> **If a sharp student asks:** with `acks=all`, "all" means all *in-sync* replicas. If the
> ISR shrinks to just the leader, "all" is one broker — which is why real durability also
> needs `min.insync.replicas=2` on the topic. We make that hands-on in Module 7.

---

## Exercise 5 — The Idempotent Producer

> **What this shows:** with retries, a lost ack causes the producer to resend a record the
> broker already wrote — a **duplicate**. `enable.idempotence=True` makes the broker
> de-duplicate the producer's retries (via a producer id + per-partition sequence number),
> giving exactly-once *delivery to the broker* while preserving order.

### 5.1 Turn it on

```python
# save as producer_idempotent.py
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,     # implies acks=all and safe retry/in-flight settings
})

for i in range(5):
    producer.produce('lab02-orders', key='user-1', value=f'idempotent-{i}')
producer.flush()
print('sent 5 idempotent records for key user-1')
```

```bash
python producer_idempotent.py
```

It runs like a normal producer — the guarantee is invisible until a retry happens. The
point is that enabling it is essentially free, so it's a sensible default for any producer
you care about.

### 5.2 Reason about the boundary

Answer these before moving on (discussion, not code):

1. If **your program** runs `producer_idempotent.py` twice, do you get duplicates in the
   topic? Why does idempotence not help here?
2. Idempotence de-dupes retries **to one partition**. Which stronger feature would you need
   to make a *consume → process → produce* step exactly-once across partitions?

> **If a sharp student asks:** what's the actual mechanism? The producer is assigned a
> Producer ID (PID); each record carries a monotonic sequence number per partition. The
> broker remembers the last sequence it accepted per (PID, partition) and silently drops a
> record whose sequence it has already seen — so a retried batch can't be written twice.

---

## Review Questions

1. `produce()` returned without error but the record never reached the topic. Give two
   reasons this can happen and the one call that would have surfaced the problem.
2. You need all events for a given `account_id` processed in order. What must you set on the
   producer, and what is the resulting guarantee's scope?
3. Raising `linger.ms` from 0 to 20 increased throughput but the average latency barely
   moved under load. Why?
4. Your team sets `acks=all` and believes data is safe on multiple brokers, but a broker
   failure still lost acknowledged records. What topic-level setting was probably missing?
5. Explain the duplicate scenario that `enable.idempotence=True` prevents, and name one
   duplicate scenario it does **not** prevent.
6. Why is `enable.idempotence=True` considered a reasonable default rather than a
   specialized option?

## What's Next

You can now produce robustly. Next is the other half of the client story:
**Module 6 (Consumer Internals)** and **Lab 03** — consumer groups and rebalancing, offset
management (auto vs. manual commit), seeking and replay, and building consumers that
survive rebalances.
