# Intermediate 2 — Consumer Internals

Elephant Scale

---

## Agenda

- Consumer groups and rebalancing; the **KIP-848** next-gen protocol
- Offset management: auto vs. manual commit; `__consumer_offsets`
- Consumer positioning, seeking, and replay
- Handling rebalances and building resilient consumers

---

## The Consumer's Job

A consumer is a **poll loop**. It asks the broker for records, processes them, and
records how far it got.

```
subscribe(['orders'])
while running:
    records = poll(timeout)     # fetch a batch
    for r in records:
        process(r)              # your business logic
    commit()                    # remember position (offset)
```

- The consumer **pulls** — the broker never pushes. You control the pace.
- Three things to get right: **group membership**, **offset commits**, and **positioning**
- Everything in this module hangs off that loop

Notes: Contrast with a push/queue system. Pull means a slow consumer can't be overwhelmed — it just polls less often. Back-pressure is built in.

---

## Consumer Groups, Recapped

From the intro: consumers sharing a `group.id` form a **group**, and Kafka assigns each
partition to **exactly one** consumer in it.

```
topic "orders" (3 partitions)     group "billing"
   P0 ─────────────────────────►  consumer-1
   P1 ─────────────────────────►  consumer-2
   P2 ─────────────────────────►  consumer-3
```

- Scales reads: more consumers → fewer partitions each → more throughput
- Ceiling = partition count (extra consumers sit idle)
- Every **group** reads the whole topic independently (pub/sub)

Now the internals: how assignment changes, and how progress is remembered.

---

## Rebalancing

When group membership changes, Kafka **rebalances** — it recomputes which consumer owns
which partition.

Triggered when:

- A consumer **joins** (you scaled up)
- A consumer **leaves** or **crashes** (misses heartbeats)
- Partitions are **added** to a subscribed topic

```
consumer-2 crashes →         rebalance →
  P0 ─► consumer-1             P0 ─► consumer-1
  P1 ─► consumer-2 ✗           P1 ─► consumer-1   (reassigned)
  P2 ─► consumer-3             P2 ─► consumer-3
```

Rebalancing keeps the group working through failures — but it isn't free, as the next
slide shows.

---

## The Cost of a Rebalance (Stop-the-World)

In the classic protocol, a rebalance is **stop-the-world**: every consumer stops, gives
up its partitions, waits for the new assignment, then resumes.

- All consumers pause processing during the rebalance → a **latency/throughput hit**
- Frequent rebalances (flapping consumers, long processing pauses) = a real problem
- Common causes: processing a batch takes longer than `max.poll.interval.ms`, so the broker
  thinks the consumer is dead and rebalances it out

Notes: This is the pain the next-gen protocol targets. If students have run Kafka consumers in anger, "rebalance storms" will be a familiar horror story.

---

## KIP-848 — The Next-Gen Rebalance Protocol

Kafka's newer **consumer group protocol (KIP-848)** makes rebalancing **incremental and
broker-coordinated** instead of stop-the-world.

- The **broker coordinator** computes assignments; consumers no longer all sync through a
  leader in lockstep
- Rebalances are **incremental** — only the partitions that must move are revoked, the rest
  keep flowing
- Less "everybody stop"; smoother scaling and recovery

**For you as a developer:** your code is the same poll loop. This is mostly a
performance/stability improvement under the hood — good to know it exists, not something you
hand-code.

Notes: Keep this conceptual. The point is awareness that modern Kafka rebalances more gracefully; students don't need to configure the protocol to benefit from the model.

---

## Offsets: Where the Group Left Off

An **offset commit** records "this group has processed up to here" for a partition. That's
how a restarted or rebalanced consumer knows where to resume.

- Committed offsets are stored **in Kafka itself** — the internal topic `__consumer_offsets`
- Committing is **per-group, per-partition**
- On startup/reassignment, a consumer **resumes from the last committed offset**

```
partition orders-1:  [0][1][2][3][4][5][6][7][8]
committed offset = 5  →  next poll returns 5,6,7,8...
```

The critical question: **when** do you commit? That choice determines your delivery
semantics.

---

## Auto-Commit

The default: the consumer commits offsets **automatically** on a timer.

```python
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'billing',
    'enable.auto.commit': True,        # default
    'auto.commit.interval.ms': 5000,   # every 5s
}
```

- **Simple** — you never call commit yourself
- But it commits on a **timer**, not on "I actually finished processing"
- Risk: offsets get committed for records you **polled but hadn't finished** when you crashed
  → those records are **skipped** on restart (**at-most-once**-ish, possible data loss)

**Fine for:** metrics, logs, best-effort. **Not fine for:** anything you can't lose.

---

## Manual Commit

Take control: turn auto-commit off and commit **after** you've processed a batch.

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

- Commit **after** processing → a crash before commit means you **re-process**, not skip
- This is **at-least-once**: no loss, but possible **duplicates** → make processing idempotent
- `commit()` can be synchronous (safe, slower) or async (faster, best-effort)

Notes: The ordering "process then commit" vs "commit then process" IS the difference between at-least-once and at-most-once. Draw it explicitly.

---

## Delivery Semantics Are a Commit Choice

The same poll loop gives different guarantees depending on **when you commit**.

| Order of operations | Semantics | On crash |
|---|---|---|
| commit **then** process | at-most-once | processed records may be lost |
| process **then** commit | at-least-once | records may be re-processed (dups) |
| process + commit **atomically** | exactly-once | needs transactions (Module 7) |

- Most real consumers want **at-least-once + idempotent processing**
- True **exactly-once** across consume→process→produce needs **transactions** — next big module

**The consumer's delivery guarantee is not a setting; it's where you put `commit()`.**

---

## Positioning: Seek and Replay

Committed offsets are just the default start point — you can **seek** a consumer to any
position and re-read.

- `seek_to_beginning()` — reprocess the whole retained log
- `seek(partition, offset)` — jump to a specific offset
- Seek by **timestamp** — "give me everything since 09:00" (offsets-for-times)
- `auto.offset.reset` (`earliest` / `latest`) — what a **brand-new** group does with no commits

```python
# replay a partition from the start
from confluent_kafka import TopicPartition
consumer.assign([TopicPartition('orders', 0, 0)])   # partition 0, offset 0
```

Notes: Replay is a superpower unique to log-based systems. Reprocess after a bug fix, backfill a new downstream, rebuild state — all just "seek and read again."

---

## Handling Rebalances in Your Code

When partitions are taken from or given to your consumer, you often need to **do something**
— commit final offsets, flush buffers, load state. That's the **rebalance listener**.

```python
def on_assign(consumer, partitions):
    print(f'assigned: {[p.partition for p in partitions]}')

def on_revoke(consumer, partitions):
    consumer.commit(asynchronous=False)     # commit before losing the partitions
    print(f'revoked: {[p.partition for p in partitions]}')

consumer.subscribe(['orders'], on_assign=on_assign, on_revoke=on_revoke)
```

- `on_revoke` fires **before** you lose partitions → your chance to commit/clean up
- `on_assign` fires when you gain partitions → seed state, log the assignment
- Committing in `on_revoke` is how you avoid re-processing a big chunk after every rebalance

---

## Building a Resilient Consumer

A checklist for consumers that survive real production:

- **Manual commit after processing** — control your delivery semantics
- **Idempotent processing** — because at-least-once means occasional duplicates
- **Commit in `on_revoke`** — don't lose progress across rebalances
- **Keep `poll()` frequent** — long processing between polls looks like a dead consumer;
  tune `max.poll.interval.ms` or process smaller batches
- **Handle `msg.error()`** every loop — not every poll returns data
- **Close cleanly** — `consumer.close()` triggers a graceful leave (faster rebalance)

Notes: This checklist is basically Lab 03. Each item maps to an exercise or a "what this shows" callout.

---

## Summary

- A consumer is a **poll loop**; the group shares partitions and **rebalances** on membership change
- **KIP-848** makes rebalances incremental and broker-coordinated — smoother, same code for you
- **Offsets** live in `__consumer_offsets`; a consumer resumes from the last committed offset
- **Auto-commit** (timer) risks loss; **manual commit after processing** gives at-least-once
- Delivery semantics = **where you put `commit()`**; exactly-once needs transactions (Module 7)
- **Seek** enables replay; **rebalance listeners** + a resilience checklist make consumers robust
