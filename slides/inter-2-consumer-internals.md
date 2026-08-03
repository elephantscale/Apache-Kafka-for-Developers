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
consumer.subscribe(List.of("orders"));
while (running) {
    records = consumer.poll(timeout);      // fetch a batch
    for (record : records)
        process(record);                   // your business logic
    consumer.commitSync();                 // remember position (offset)
}
```

- The consumer **pulls** — the broker never pushes. You control the pace.
- Three things to get right: **group membership**, **offset commits**, and **positioning**
- Everything in this module hangs off that loop

Notes:
Contrast with a push/queue system. Pull means a slow consumer can't be overwhelmed — it just polls less often. Back-pressure is built in.

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

Notes:
This is the pain the next-gen protocol targets. If students have run Kafka consumers in anger, "rebalance storms" will be a familiar horror story.

---

## KIP-848 — The Next-Gen Rebalance Protocol

Kafka's newer **consumer group protocol (KIP-848)** makes rebalancing **incremental and
broker-coordinated** instead of stop-the-world.

- The **broker coordinator** computes assignments; consumers no longer all sync through a
  leader in lockstep
- Rebalances are **incremental** — only the partitions that must move are revoked, the rest
  keep flowing
- Less "everybody stop"; smoother scaling and recovery

**It is GA in Kafka 4 — but opt-in per consumer.** The default is still the classic protocol:

```java
c.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");   // default: "classic"
```

**For you as a developer:** your poll loop, your commits, your rebalance listener — all
unchanged. You flip one config and the group coordinates differently underneath.

Notes:
Correct the common half-truth here: KIP-848 is GA in Kafka 4, but a consumer does *not* get it automatically — `group.protocol` still defaults to `classic`. The broker side must also allow it (`group.coordinator.rebalance.protocols`); our lab compose already enables `classic,consumer`, so students can try the flip. Caveat worth stating: a group must be either all-classic or all-consumer during migration, and `onPartitionsLost` semantics differ slightly — which is why teams migrate deliberately rather than flipping in place.

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

```java
Properties c = new Properties();
c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
c.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);       // default
c.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);  // every 5s
```

- **Simple** — you never call commit yourself
- But it commits on a **timer**, not on "I actually finished processing"
- Risk: offsets get committed for records you **polled but hadn't finished** when you crashed
  → those records are **skipped** on restart (**at-most-once**-ish, possible data loss)

**Fine for:** metrics, logs, best-effort. **Not fine for:** anything you can't lose.

---

## Manual Commit

Take control: turn auto-commit off and commit **after** you've processed a batch.

```java
c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

try (Consumer<String, String> consumer = new KafkaConsumer<>(c)) {
  consumer.subscribe(List.of("orders"));

  while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> r : records) {
      process(r);                  // do the work FIRST
    }
    consumer.commitSync();         // THEN record progress for the whole batch
  }
}
```

- Commit **after** processing → a crash before commit means you **re-process**, not skip
- This is **at-least-once**: no loss, but possible **duplicates** → make processing idempotent
- `commitSync()` blocks and retries (safe, slower); `commitAsync()` returns immediately
  (faster, best-effort) — a common pattern is async in the loop, sync on shutdown

Notes:
The ordering "process then commit" vs "commit then process" IS the difference between at-least-once and at-most-once. Draw it explicitly.

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

**The consumer's delivery guarantee is not a setting; it's where you put `commitSync()`.**

---

## Positioning: Seek and Replay

Committed offsets are just the default start point — you can **seek** a consumer to any
position and re-read.

- `seekToBeginning(partitions)` — reprocess the whole retained log
- `seek(topicPartition, offset)` — jump to a specific offset
- `offsetsForTimes(...)` — seek by **timestamp**: "give me everything since 09:00"
- `auto.offset.reset` (`earliest` / `latest`) — what a **brand-new** group does with no commits

```java
// replay partition 0 from the start
TopicPartition tp = new TopicPartition("orders", 0);
consumer.assign(List.of(tp));        // manual assignment — no group, no rebalance
consumer.seekToBeginning(List.of(tp));
```

Notes:
Replay is a superpower unique to log-based systems. Reprocess after a bug fix, backfill a new downstream, rebuild state — all just "seek and read again."

---

## Handling Rebalances in Your Code

When partitions are taken from or given to your consumer, you often need to **do something**
— commit final offsets, flush buffers, load state. That's the **rebalance listener**.

```java
consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    consumer.commitSync();                  // commit BEFORE losing the partitions
    System.out.println("revoked: " + partitions);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    System.out.println("assigned: " + partitions);
  }
});
```

- `onPartitionsRevoked` fires **before** you lose partitions → your chance to commit/clean up
- `onPartitionsAssigned` fires when you gain partitions → seed state, log the assignment
- Committing on revoke is how you avoid re-processing a big chunk after every rebalance

Notes:
There is a third callback, `onPartitionsLost`, for when partitions are taken away *without* a clean revoke (the consumer was already fenced out). Don't commit there — you no longer own them. The default implementation just calls `onPartitionsRevoked`, which is usually wrong.

---

## Building a Resilient Consumer

A checklist for consumers that survive real production:

- **Manual commit after processing** — control your delivery semantics
- **Idempotent processing** — because at-least-once means occasional duplicates
- **Commit in `onPartitionsRevoked`** — don't lose progress across rebalances
- **Keep `poll()` frequent** — long processing between polls looks like a dead consumer;
  tune `max.poll.interval.ms` or lower `max.poll.records` to shrink the batch
- **Expect empty polls and exceptions** — `poll()` often returns zero records; deserialization
  and auth failures arrive as **thrown exceptions**, so wrap the loop
- **Close cleanly** — `consumer.close()` (or try-with-resources) triggers a graceful leave,
  so the group rebalances immediately instead of waiting out the session timeout

Notes:
This checklist is basically Lab 03. Each item maps to an exercise or a "what this shows" callout.

---

## Summary

- A consumer is a **poll loop**; the group shares partitions and **rebalances** on membership change
- **KIP-848** makes rebalances incremental and broker-coordinated — GA in Kafka 4, opt in with
  `group.protocol=consumer`; your code is unchanged
- **Offsets** live in `__consumer_offsets`; a consumer resumes from the last committed offset
- **Auto-commit** (timer) risks loss; **manual commit after processing** gives at-least-once
- Delivery semantics = **where you put `commitSync()`**; exactly-once needs transactions (Module 7)
- **Seek** enables replay; **rebalance listeners** + a resilience checklist make consumers robust
