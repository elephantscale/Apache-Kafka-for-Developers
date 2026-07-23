# Lab 4 — Delivery Semantics & Transactions

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 3 — Delivery Semantics & Transactions
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate / Advanced
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

## Objectives

By the end of this lab you will be able to:

- Use a transactional producer (`transactional.id`, `init_transactions`, begin/commit/abort)
- See how an aborted transaction is hidden from a `read_committed` consumer
- Build a consume-process-produce pipeline that commits input offsets *inside* the transaction
- Prove the pipeline is exactly-once by crashing it mid-batch and restarting
- Explain the boundary: exactly-once inside Kafka vs. idempotent external side effects

## Prerequisites

- The core cluster running (`docker compose up -d`, three brokers healthy)
- Python venv active with `confluent-kafka` (see [`labs/SETUP.md`](../SETUP.md))
- Labs 02–03 completed (idempotent producer; manual-commit consumer)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no ZooKeeper,
> no Kubernetes). The transaction-state internal topic is replicated across all three brokers
> (`transaction.state.log.replication.factor=3`, `min.isr=2`) — already configured in the
> lab compose file, so transactions work out of the box. Your code runs on the host against
> `localhost:9092`.

### Create the lab topics

```bash
for t in lab04-input lab04-output; do
  docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $t --partitions 3 --replication-factor 3
done
```

Seed some input:

```python
# save as seed_input.py  — usage: python seed_input.py <count>
import sys, json
from confluent_kafka import Producer
n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
p = Producer({'bootstrap.servers': 'localhost:9092'})
for i in range(n):
    p.produce('lab04-input', key=f'acct-{i % 4}',
              value=json.dumps({'id': i, 'amount': 10 + i}).encode())
p.flush()
print(f'seeded {n} input records')
```

```bash
python seed_input.py 20
```

---

## Exercise 1 — A Transactional Producer

> **What this shows:** the transactional producer lifecycle. A stable `transactional.id`
> gives the producer a durable identity; `init_transactions()` registers it; then work is
> wrapped in `begin_transaction()` / `commit_transaction()`. Records become visible to
> `read_committed` readers only at commit.

### 1.1 Commit a transaction

```python
# save as txn_producer.py
import json
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'lab04-txn-producer',   # stable identity
    # enable.idempotence is implied by transactional.id
})
producer.init_transactions()

producer.begin_transaction()
try:
    for i in range(5):
        producer.produce('lab04-output',
                          key=f'acct-{i}',
                          value=json.dumps({'id': i, 'status': 'CONFIRMED'}).encode())
    producer.commit_transaction()
    print('committed 5 records in one transaction')
except Exception as e:
    producer.abort_transaction()
    print(f'aborted: {e}')
```

```bash
python txn_producer.py
```

### 1.2 Read them back (committed)

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed
```

All five appear — they were committed atomically.

> **If a sharp student asks:** what does `init_transactions()` actually do? It registers the
> `transactional.id` with the transaction coordinator and **fences** any earlier producer
> instance using the same id (bumping an epoch), so a zombie predecessor can no longer commit.
> It also recovers/aborts any in-flight transaction left by a crashed prior run.

---

## Exercise 2 — Abort Is Invisible to `read_committed`

> **What this shows:** an aborted transaction's records physically exist in the log, but a
> `read_committed` consumer never sees them, while a `read_uncommitted` consumer does. This is
> the mechanism that makes exactly-once safe on the read side.

### 2.1 Produce a committed batch and an aborted batch

```python
# save as txn_abort.py
import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092',
                     'transactional.id': 'lab04-abort-demo'})
producer.init_transactions()

# batch 1 — COMMIT
producer.begin_transaction()
for i in range(3):
    producer.produce('lab04-output', value=json.dumps({'batch': 1, 'i': i}).encode())
producer.commit_transaction()
print('committed batch 1')

# batch 2 — ABORT
producer.begin_transaction()
for i in range(3):
    producer.produce('lab04-output', value=json.dumps({'batch': 2, 'i': i}).encode())
producer.abort_transaction()
print('aborted batch 2')
```

```bash
python txn_abort.py
```

### 2.2 Compare the two isolation levels

```bash
echo "=== read_committed (should NOT see batch 2) ==="
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep '"batch": 2' && echo "LEAK!" || echo "batch 2 correctly hidden"

echo "=== read_uncommitted (WILL see batch 2) ==="
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_uncommitted | grep -c '"batch": 2'
```

`read_committed` hides the aborted batch; `read_uncommitted` shows it.

> **If a sharp student asks:** if aborted records are still written to disk, isn't that waste?
> A little — the broker writes the records plus an abort marker, and compaction/retention
> eventually reclaim them. The design keeps the append-only log simple: nothing is ever
> rewritten in place; readers just filter using the markers.

---

## Exercise 3 — Consume-Process-Produce (Exactly-Once)

> **What this shows:** the canonical exactly-once pipeline. The consumer does **not**
> auto-commit; instead the producer binds the input offsets into the same transaction as the
> output, so outputs and the offset advance commit atomically.

### 3.1 The pipeline

```python
# save as pipeline_eos.py
import json, sys
from confluent_kafka import Consumer, Producer, TopicPartition

crash_after = int(sys.argv[1]) if len(sys.argv) > 1 else -1   # -1 = never crash

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab04-eos',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,          # offsets committed ONLY via the transaction
})
consumer.subscribe(['lab04-input'])

producer = Producer({'bootstrap.servers': 'localhost:9092',
                     'transactional.id': 'lab04-eos-pipeline'})
producer.init_transactions()

processed = 0
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue

    producer.begin_transaction()
    try:
        record = json.loads(msg.value())
        enriched = {'id': record['id'], 'amount': record['amount'], 'tax': record['amount'] * 0.1}
        producer.produce('lab04-output', key=str(record['id']),
                         value=json.dumps(enriched).encode())

        # bind THIS input offset into the transaction
        offsets = [TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)]
        producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())

        # optional fault injection BEFORE commit
        processed += 1
        if processed == crash_after:
            print(f'CRASH before committing record {record["id"]}')
            import os; os._exit(1)

        producer.commit_transaction()
        print(f'committed id={record["id"]}  (processed={processed})')
    except Exception as e:
        producer.abort_transaction()
        print(f'aborted: {e}')
```

### 3.2 Run it clean

```bash
python seed_input.py 20
python pipeline_eos.py        # Ctrl-C once "committed" lines stop appearing
```

Check the output count:

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep -c '"tax"'
```

---

## Exercise 4 — Prove Exactly-Once Under a Crash

> **What this shows:** the payoff. Crash the pipeline *after producing output but before
> committing* the transaction. Because output + offset commit are atomic, the aborted output
> is invisible and the input offset never advanced — on restart the record is reprocessed and
> the downstream sees it **exactly once**, not twice.

### 4.1 Reset and crash mid-stream

Use a fresh output topic so the count is unambiguous:

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic lab04-output
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab04-output --partitions 3 --replication-factor 3

# fresh consumer group so we read all 20 inputs from the start
```

Edit `pipeline_eos.py`'s `group.id` to `lab04-eos-crash`, then:

```bash
python seed_input.py 20
python pipeline_eos.py 5      # crashes right before committing the 5th record
python pipeline_eos.py        # restart; runs to completion, Ctrl-C when idle
```

### 4.2 Count the output

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep -c '"tax"'
```

You should get **exactly 20** — not 24. The 5th record's aborted output was discarded and its
offset never advanced, so the restart reprocessed it once. No duplicates, no loss.

> **If a sharp student asks:** what if it crashes *after* `commit_transaction()` returns but
> before the next poll? Nothing is lost — the offset was committed inside the transaction, so
> the restart simply resumes at the next record. The atomic unit is exactly "outputs + offset,"
> which is why neither ordering of the crash produces a duplicate.

---

## Exercise 5 — The Boundary (Discussion + Mini-Demo)

> **What this shows:** exactly-once is a guarantee *within Kafka*. A side effect to an external
> system (DB, email, REST) is **not** part of the Kafka transaction — so you make those
> idempotent instead.

Consider adding an external write to the pipeline:

```python
# INSIDE the try, alongside produce():
#   db.insert(enriched)          # <-- NOT in the Kafka transaction!
```

Discuss before moving on:

1. If the transaction aborts *after* `db.insert()` ran, what's now inconsistent between the DB
   and the Kafka output?
2. Rewrite `db.insert(enriched)` as an idempotent operation so a reprocess is harmless. (Hint:
   upsert on a natural key like `id`.)
3. Why can't Kafka simply "include" the database write in its transaction?

> **If a sharp student asks:** isn't there Kafka-to-DB exactly-once with connectors? Some sink
> connectors achieve effectively-once by tracking offsets in the destination or using
> idempotent upserts — but that's the connector implementing idempotency at the edge, not the
> Kafka transaction reaching into the database. The boundary still holds.

---

## Review Questions

1. What two things does a transaction make atomic in a consume-process-produce loop, and why
   must the consumer have `enable.auto.commit=False`?
2. What is the `transactional.id` for, and what failure does producer "fencing" prevent?
3. A downstream team reads your transactional output but still sees duplicated/aborted records.
   What single consumer setting did they most likely miss?
4. In Exercise 4 you got exactly 20 outputs after a mid-stream crash. Explain, in terms of the
   atomic unit, why it wasn't 24.
5. Your pipeline also writes each result to PostgreSQL. Does Kafka's exactly-once cover that
   write? What technique makes the overall effect correct?
6. When would you deliberately choose at-least-once over exactly-once, despite the duplicates?

## What's Next

You've mastered correctness for raw bytes. Next, correctness for **data shape**:
**Module 8 (Serialization & the Schema Registry)** and **Lab 05** — Avro/Protobuf/JSON Schema,
registering schemas, schema IDs and serdes, and evolving a schema without breaking consumers.
