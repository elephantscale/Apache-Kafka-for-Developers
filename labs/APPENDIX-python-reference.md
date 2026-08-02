# Appendix — Python Reference

- **Course:** Apache Kafka for Developers
- **Status:** Optional reference. The course labs are **Java**; nothing here is required.

## What this is

Every producer/consumer/transaction example from Modules 5–7 and Labs 02–04, written with
the **`confluent-kafka`** Python client instead of the Kafka Java client.

It exists for two reasons:

- Developers who think in Python can read the Kafka concept in a familiar syntax while doing
  the lab in Java.
- Teams that later build tooling or prototypes in Python have a working starting point.

The Kafka **concepts are identical** in both clients — the same configs (`acks`,
`enable.idempotence`, `linger.ms`, `isolation.level`), the same partitioner, the same
delivery semantics. Only the API surface differs. Where the Java client uses typed config
constants (`ProducerConfig.ACKS_CONFIG`), the Python client takes a plain config dict with
the wire names (`'acks'`).

> Lab 01 has no entry here — it is CLI-based (`kafka-topics.sh`, `kafka-console-consumer.sh`)
> and identical regardless of language.

## Python environment

```bash
cd <repo root>
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "confluent-kafka[avro,schemaregistry]" requests
```

Re-run `source .venv/bin/activate` at the start of each session.

Smoke test (venv active, cluster up):

```bash
python -c "from confluent_kafka.admin import AdminClient; \
print(AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5).brokers)"
```

Should print three broker entries.

> This installs from PyPI at run time. If a class is likely to use the Python path, install
> it into the lab image ahead of time rather than during the session.

## Maintainer note

These blocks are the course's own pre-port code, recovered from git history — slides from
`652792d^`, labs from `3af8e71^`. If you ever need more context around one, `git show` those
paths.

**Do not restore the old files wholesale.** Substantial fixes landed *after* the port
(`98608f8`, `b653f56`, `6e66b7c`, `ff92f10`, `d4cb9a5`, `5284335`, `a904a21`, `d6ec31e`) and
checking out a pre-port path would silently revert them. Copy individual blocks instead.

---

## Slides — Intermediate 1: Producer Internals

*Source: `git show 652792d^:slides/inter-1-producer-internals.md` — 4 block(s)*


### Stage 1: Serialization

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


### Stage 3b: Compression

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 10,
    'batch.size': 65536,
    'compression.type': 'zstd',
})
```


### Idempotent Producer

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,     # exactly-once *at the producer*
    'acks': 'all',                  # required; set implicitly
})
```


### A Tuned Producer, End to End

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


---

## Slides — Intermediate 2: Consumer Internals

*Source: `git show 652792d^:slides/inter-2-consumer-internals.md` — 4 block(s)*


### Auto-Commit

```python
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'billing',
    'enable.auto.commit': True,        # default
    'auto.commit.interval.ms': 5000,   # every 5s
}
```


### Manual Commit

```python
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'billing',
    'enable.auto.commit': False,
}
consumer = Consumer(conf)
consumer.subscribe(['orders'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        handle(msg.error()); continue
    process(msg)                 # do the work FIRST
    consumer.commit(msg)         # THEN record progress
```


### Positioning: Seek and Replay

```python
# replay a partition from the start
from confluent_kafka import TopicPartition
consumer.assign([TopicPartition('orders', 0, 0)])   # partition 0, offset 0
```


### Handling Rebalances in Your Code

```python
def on_assign(consumer, partitions):
    print(f'assigned: {[p.partition for p in partitions]}')

def on_revoke(consumer, partitions):
    consumer.commit(asynchronous=False)     # commit before losing the partitions
    print(f'revoked: {[p.partition for p in partitions]}')

consumer.subscribe(['orders'], on_assign=on_assign, on_revoke=on_revoke)
```


---

## Slides — Intermediate 3: Delivery Semantics & Transactions

*Source: `git show 652792d^:slides/inter-3-delivery-transactions.md` — 3 block(s)*


### The `transactional.id`

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'orders-enricher-1',   # stable across restarts
    'enable.idempotence': True,                 # implied by transactional.id
})
producer.init_transactions()                    # once, at startup
```


### The Consume-Process-Produce Loop

```python
producer.init_transactions()

while True:
    msgs = consume_batch()
    producer.begin_transaction()
    try:
        for m in msgs:
            result = process(m)
            producer.produce('output', value=result)
        # bind the INPUT offsets to THIS transaction
        producer.send_offsets_to_transaction(
            offsets_of(msgs), consumer.consumer_group_metadata())
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()            # nothing leaks; safe to retry
```


### The Consumer Side: `read_committed`

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'downstream',
    'isolation.level': 'read_committed',    # default is read_uncommitted
})
```


---

## Lab 02 — Producer Internals & Tuning

*Source: `git show 3af8e71^:labs/02-Producers/lab-02-producers.md` — 6 block(s)*


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


### 2.2 Now go keyless

```python
    producer.produce('lab02-orders', value=f'event-{i}', callback=report)
```


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


---

## Lab 03 — Consumer Internals

*Source: `git show 3af8e71^:labs/03-Consumers/lab-03-consumers.md` — 6 block(s)*


### Create the lab topic and a feeder

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


---

## Lab 04 — Delivery Semantics & Transactions

*Source: `git show 3af8e71^:labs/04-Transactions/lab-04-transactions.md` — 5 block(s)*


### Create the lab topics

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


### Exercise 5 — The Boundary (Discussion + Mini-Demo)

```python
# INSIDE the try, alongside produce():
#   db.insert(enriched)          # <-- NOT in the Kafka transaction!
```

