# Lab 3 — Consumer Groups & Offsets

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 2 — Consumer Internals
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

## Objectives

By the end of this lab you will be able to:

- Write a Python consumer with a proper poll loop and error handling
- See how auto-commit can skip records, and fix it with manual commit
- Choose commit ordering to get at-least-once behavior, and reason about duplicates
- Seek and replay a partition from an arbitrary offset
- Use a rebalance listener to commit offsets before losing partitions

## Prerequisites

- The core cluster running (`docker compose up -d`, three brokers healthy)
- Python venv active with `confluent-kafka` (see [`labs/SETUP.md`](../SETUP.md))
- Lab 02 completed (you have a working producer)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no ZooKeeper,
> no Kubernetes). Your Python code runs on the host and connects to `localhost:9092`; Kafka
> CLI tools run inside the brokers via `docker exec kafka-1 …`. Save each file in your
> working directory and run it with the venv active.

### Create the lab topic and a feeder

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab03-events --partitions 3 --replication-factor 3
```

We'll drive it with a small producer you can re-run whenever a topic needs data:

```python
# save as feed.py  — usage: python feed.py <count>
import sys, json
from confluent_kafka import Producer

n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
p = Producer({'bootstrap.servers': 'localhost:9092'})
for i in range(n):
    p.produce('lab03-events', key=f'user-{i % 5}',
              value=json.dumps({'seq': i}).encode())
p.flush()
print(f'produced {n} events')
```

```bash
python feed.py 100
```

---

## Exercise 1 — A Proper Poll Loop

> **What this shows:** a consumer is a loop of `poll → process → (commit)`. `poll()` returns
> one message or `None` (a timeout with no data), and you must check `msg.error()` every
> iteration — not every poll is a record.

### 1.1 Write the consumer

```python
# save as consumer_basic.py
import json
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab03-basic',
    'auto.offset.reset': 'earliest',    # new group with no commits: start at 0
})
consumer.subscribe(['lab03-events'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue                    # timeout, no message this cycle
        if msg.error():
            print(f'error: {msg.error()}')
            continue
        data = json.loads(msg.value())
        print(f'P{msg.partition()} @ {msg.offset()}  seq={data["seq"]}')
except KeyboardInterrupt:
    pass
finally:
    consumer.close()                    # graceful leave -> faster rebalance
```

### 1.2 Run it

```bash
python feed.py 100      # ensure there's data
python consumer_basic.py
```

You'll see all 100 events, labeled by partition and offset. Stop with `Ctrl-C`.

### 1.3 Run it again

Start it a second time. With **no new data** it prints nothing new — the group's committed
offsets are at the end. That's auto-commit having saved your position (default
`enable.auto.commit=True`).

> **If a sharp student asks:** why `auto.offset.reset='earliest'`? It only applies the
> **first** time a group runs (no committed offset yet). After that, the committed offset
> wins. Change the `group.id` to see it re-read from the beginning as a brand-new group.

---

## Exercise 2 — How Auto-Commit Can Lose Data

> **What this shows:** auto-commit commits on a **timer**, not when your work is done. If you
> crash after the timer commits but before processing finishes, those records are **skipped**
> on restart — silent data loss.

### 2.1 Simulate slow processing + a crash

```python
# save as consumer_autocommit_loss.py
import json, time, os
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab03-autoloss',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,     # commits every 1s
})
consumer.subscribe(['lab03-events'])

count = 0
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    # Pretend processing is slow. Auto-commit may fire DURING this sleep,
    # committing offsets for records we haven't finished.
    time.sleep(0.5)
    data = json.loads(msg.value())
    count += 1
    print(f'processed seq={data["seq"]} (count={count})')
    if count == 5:
        print('CRASH before finishing the batch!')
        os._exit(1)                      # hard exit — no clean commit/close
```

### 2.2 Run, crash, restart

```bash
python feed.py 20
python consumer_autocommit_loss.py     # processes ~5, then hard-exits
python consumer_autocommit_loss.py     # restart — note where it resumes
```

On restart it likely **skips ahead**, past records it never actually finished — because the
timer had already committed those offsets. That gap is lost data.

> **If a sharp student asks:** is auto-commit always unsafe? No — it's fine when losing a few
> records doesn't matter (metrics, logs). The problem is only when "committed" must mean
> "processed." For that, commit manually.

---

## Exercise 3 — Manual Commit for At-Least-Once

> **What this shows:** turn auto-commit off and commit **after** processing. Now a crash
> before commit causes **re-processing**, not skipping — at-least-once. The price is possible
> duplicates, so processing should be idempotent.

### 3.1 Process first, then commit

```python
# save as consumer_manual.py
import json, time, os
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab03-manual',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,         # WE decide when to commit
})
consumer.subscribe(['lab03-events'])

count = 0
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    data = json.loads(msg.value())       # 1. process
    time.sleep(0.2)
    count += 1
    print(f'processed seq={data["seq"]} (count={count})')
    consumer.commit(msg)                 # 2. THEN commit (sync)
    if count == 5:
        print('CRASH after processing 5, some already committed')
        os._exit(1)
```

### 3.2 Run, crash, restart

```bash
python feed.py 20
python consumer_manual.py      # processes 5, crashes
python consumer_manual.py      # restart
```

On restart it resumes at the **last committed** record — no records are skipped. You may see
one record processed **twice** (processed but the crash beat a later commit): that's
at-least-once, and exactly why processing must be idempotent.

> **If a sharp student asks:** sync vs async commit? `commit(msg)` (sync) blocks until the
> broker confirms — safest. `commit(msg, asynchronous=True)` is faster but best-effort; you'd
> pair it with a final sync commit on shutdown. Committing per-message is simplest to reason
> about; batching commits is a throughput optimization.

---

## Exercise 4 — Seek and Replay

> **What this shows:** committed offsets are only the *default* start. You can position a
> consumer anywhere in the retained log and re-read — the basis of replay, backfill, and
> reprocess-after-bugfix.

### 4.1 Replay a partition from the beginning

```python
# save as consumer_replay.py
import json
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab03-replay',
    'enable.auto.commit': False,
})

# Assign partition 0 explicitly and rewind to offset 0 (ignore any commits)
tp = TopicPartition('lab03-events', 0, 0)
consumer.assign([tp])
consumer.seek(tp)

n = 0
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break                            # drained
    if msg.error():
        continue
    n += 1
    print(f'replayed P0 @ {msg.offset()}  {json.loads(msg.value())}')
consumer.close()
print(f'replayed {n} records from partition 0')
```

```bash
python consumer_replay.py
```

It re-reads partition 0 from offset 0, regardless of what any group committed.

> **If a sharp student asks:** how would I replay "everything since 9 AM"? Use
> `offsets_for_times()` to convert a timestamp to the first offset at/after it, then `seek()`
> there. Same mechanism, timestamp instead of a literal offset.

---

## Exercise 5 — Commit on Rebalance

> **What this shows:** when a rebalance revokes your partitions, anything processed-but-not-
> committed would be re-processed by whoever picks them up. A rebalance listener lets you
> commit final offsets in `on_revoke`, right before the partitions leave.

### 5.1 Add a rebalance listener

```python
# save as consumer_rebalance.py
import json
from confluent_kafka import Consumer

def on_assign(c, partitions):
    print(f'ASSIGN  {[p.partition for p in partitions]}')

def on_revoke(c, partitions):
    print(f'REVOKE  {[p.partition for p in partitions]} — committing first')
    try:
        c.commit(asynchronous=False)     # flush progress before losing partitions
    except Exception as e:
        print(f'  commit on revoke failed: {e}')

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'lab03-rebalance',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
consumer.subscribe(['lab03-events'], on_assign=on_assign, on_revoke=on_revoke)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        print(f'P{msg.partition()} @ {msg.offset()}  {json.loads(msg.value())["seq"]}')
        consumer.commit(msg)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

### 5.2 Trigger a rebalance

```bash
python feed.py 300
# terminal A:
python consumer_rebalance.py
# terminal B (same group — start while A runs):
python consumer_rebalance.py
```

Watch terminal A: when B joins, A prints **REVOKE** for the partitions it gives up (committing
first), then **ASSIGN** for what it keeps. Stop B and A gets them back — another rebalance.

Inspect the group from a third terminal:

```bash
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group lab03-rebalance
```

> **If a sharp student asks:** with the KIP-848 next-gen protocol, do I still need this? Yes —
> the protocol makes rebalances *incremental and cheaper*, but the same lifecycle hooks apply.
> Committing before you lose a partition is good practice regardless of protocol.

---

## Review Questions

1. `poll()` returned `None`. Does that mean the topic is empty? What should your loop do?
2. A team uses default auto-commit and reports that after every crash "a few events go
   missing." Explain the mechanism and the one-line config change that fixes it.
3. You switched to manual commit and now occasionally see an event processed twice. Is this a
   bug? What property must your processing have, and why?
4. Put these in order for at-least-once delivery: `commit`, `process`, `poll`. Then give the
   order that produces at-most-once.
5. You deployed a fix and need to reprocess yesterday's data for one partition. Which two API
   calls do you use, and how would you start "from 9 AM" instead of offset 0?
6. Why commit offsets inside `on_revoke`? What goes wrong if you don't?

## What's Next

You can produce and consume with real delivery control. Next you'll close the loop for true
end-to-end correctness: **Module 7 (Delivery Semantics & Transactions)** and **Lab 04** —
at-most/at-least/exactly-once, transactional producers, the consume-process-produce pattern,
and `read_committed` consumers.
