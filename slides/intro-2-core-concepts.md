# Intro 2 — Core Concepts

Elephant Scale

---

## Agenda

- Topics, partitions, and offsets — the log abstraction
- Producers, consumers, brokers — and translating Kafka's vocabulary
- Consumer groups and parallelism
- Replication, leaders, and fault tolerance (ISR) at a high level
- Retention: time, size, and log compaction

---

## The One Idea: A Topic Is a Log

Everything in Kafka is built on one data structure: an **append-only log**.

- A **topic** is a named log — e.g. `orders`, `clicks`, `payments`
- Producers **append** events to the end; consumers **read** forward from any position
- Events are **immutable** — once written, never changed
- Reads don't remove data — many consumers read the same log independently

```
topic "orders"  (oldest ──────────────────────► newest)
   ┌────┬────┬────┬────┬────┬────┬────┐
   │ e0 │ e1 │ e2 │ e3 │ e4 │ e5 │ e6 │◄── producer appends here
   └────┴────┴────┴────┴────┴────┴────┘
     ▲                        ▲
  consumer A               consumer B   (read independently)
```

Notes:
If students remember only one thing today, it's "topic = append-only log." Partitions, offsets, and consumer groups all follow from this.

---

## Partitions — Splitting the Log for Scale

A single log on one machine can't scale. So Kafka splits each topic into
**partitions**, and spreads them across brokers.

- A topic has **N partitions**, chosen when you create it
- Each partition is its **own ordered log**, on some broker
- Partitions give Kafka its **parallelism** and **capacity** — more partitions, more throughput

```
topic "orders" with 3 partitions
  P0:  ┌────┬────┬────┬────┐
       │ e0 │ e3 │ e6 │ e9 │
       └────┴────┴────┴────┘
  P1:  ┌────┬────┬────┐
       │ e1 │ e4 │ e7 │
       └────┴────┴────┘
  P2:  ┌────┬────┬────┐
       │ e2 │ e5 │ e8 │
       └────┴────┴────┘
```

**Key trade-off:** ordering is guaranteed **within a partition**, never across partitions.

---

## Offsets — A Message's Address

Within a partition, every event gets a monotonically increasing **offset**: 0, 1,
2, 3, …

- An offset is **per-partition** — partition 0's offset 5 is unrelated to partition 1's offset 5
- A message is uniquely identified by **(topic, partition, offset)**
- Offsets never go backward and are never reused within a partition
- A consumer's **position** is just "the next offset I will read"

```
  P1:  ┌────┬────┬────┬────┬────┐
       │ e1 │ e4 │ e7 │e10 │e13 │
       └────┴────┴────┴────┴────┘
offset:  0    1    2    3    4
                        ▲
              consumer position = 3
              (has read 0,1,2; next is 3)
```

Notes:
This is the mechanism behind replay — seek a consumer back to an earlier offset and it re-reads history. We'll do exactly that in the labs.

---

## Keys and Partitioning

How does a producer decide which partition an event goes to? By the **message key**.

- **With a key** — Kafka hashes the key; the same key always lands in the same partition
  - `order_id=1234` → always partition 2 → all events for that order stay **ordered**
- **Without a key** — events are spread across partitions (round-robin-ish) for balance

```
  key "user-42"  ─hash─►  P1   (every "user-42" event, in order)
  key "user-99"  ─hash─►  P0
  no key         ─────►   P0, P1, P2, ...  (balanced)
```

**This is the central design decision for a producer:** pick a key when you need
per-entity ordering (per user, per order, per device); go keyless when you just
want even spread.

---

## Producers, Consumers, Brokers

Three roles make up the system:

- **Broker** — a Kafka server. It stores partitions on disk and serves reads/writes.
  A **cluster** is several brokers working together.
- **Producer** — a client that **appends** events to topic partitions.
- **Consumer** — a client that **reads** events forward from partitions.

```
   Producers                Cluster (brokers)             Consumers
  ┌────────┐        ┌──────────┬──────────┬──────────┐   ┌────────┐
  │ app A  │───────►│ broker 1 │ broker 2 │ broker 3 │──►│ app X  │
  │ app B  │───────►│  P0,P3   │  P1,P4   │  P2,P5   │──►│ app Y  │
  └────────┘        └──────────┴──────────┴──────────┘   └────────┘
```

Producers and consumers are **your code** (Java in this course). Brokers are the
Kafka cluster — for us, three Docker containers.

---

## If Kafka Were Named Today

Kafka's vocabulary is from 2010 and predates today's streaming and database conventions.
If you come from other systems, this translation makes it click faster:

| Kafka says | Everyone else says |
|---|---|
| **Broker** | Server, or Node |
| **Producer** | Publisher, or Writer |
| **Consumer** | Subscriber, or Reader |
| **Topic** | Stream, or Channel |
| **Partition** | **Shard** — a horizontal slice, exactly the database idea |
| **Offset** | **Cursor** — your position in the log |
| **Consumer Group** | Subscription, or a worker pool sharing the load |

> The names are historical, not technical destiny — the **concepts** are what matter.
> When a term feels odd, translate it in your head.

Notes:
This slide saves an hour of confusion later. "Partition = shard" and "offset = cursor" are the two that unlock the most — developers already own both concepts, they just don't recognize them under Kafka's names. Invite the room to name the equivalent in whatever they use today (JMS, MQ, SQS, a database read cursor).

---

## Consumer Groups — Scaling Consumption

One consumer may not keep up with a busy topic. A **consumer group** lets several
consumers share the work.

- Consumers that share a `group.id` form **one group**
- Kafka **assigns each partition to exactly one consumer** in the group
- Add consumers → each handles fewer partitions → more throughput
- The group tracks its progress via **committed offsets**

```
topic "orders" (3 partitions)     group "billing"
   P0 ─────────────────────────►  consumer 1
   P1 ─────────────────────────►  consumer 2
   P2 ─────────────────────────►  consumer 3
```

Notes:
This is the heart of Kafka's scaling model on the read side. The next slide shows the limit and what "different groups" means.

---

## Parallelism: Partitions Are the Unit

The number of **partitions** caps the parallelism of a single consumer group.

- 3 partitions → at most **3 active consumers** in a group; a 4th sits **idle**
- More consumers than partitions doesn't help — one consumer per partition is the max
- So **partition count is a scaling decision** you make up front

```
3 partitions, 4 consumers in one group:
   P0 ─► consumer 1
   P1 ─► consumer 2
   P2 ─► consumer 3
        consumer 4   (idle — no partition to assign)
```

**Rule of thumb:** choose partition count for the parallelism you'll eventually
need — it's easy to add partitions, awkward to remove them.

---

## Same Data, Many Consumers: Different Groups

A partition goes to one consumer **within a group** — but **every group reads the
whole topic independently**.

```
topic "orders"
   ├──► group "billing"    (reads all events, tracks its own offsets)
   ├──► group "analytics"  (reads all events, its own offsets)
   └──► group "fraud"      (reads all events, its own offsets)
```

- This is **pub/sub**: add a new group and it sees the full stream, with **zero producer changes**
- Each group's progress (offsets) is completely separate
- One team's slow consumer never affects another team's group

This is how one topic feeds many independent applications at once.

---

## Replication — Surviving Broker Failure

Each partition is **replicated** across brokers so no single failure loses data.

- **Replication factor** = number of copies (e.g. RF=3 → 3 copies on 3 brokers)
- One replica is the **leader** — all reads and writes go through it
- The others are **followers** — they copy the leader's log
- If the leader's broker dies, a follower is **promoted** to leader automatically

```
partition "orders-0", RF=3
   broker 1:  LEADER    ◄── producers/consumers talk here
   broker 2:  follower  ── replicates from leader
   broker 3:  follower  ── replicates from leader
        (broker 1 dies → broker 2 becomes leader)
```

---

## ISR — In-Sync Replicas

The **ISR** is the set of replicas that are **caught up** with the leader.

- A follower that keeps up is **in-sync**; one that falls behind drops out of the ISR
- Only an in-sync replica is eligible to become leader — so no acknowledged data is lost
- Producers can require a write to reach a minimum number of in-sync replicas before it's
  considered durable (we'll tune `acks` and `min.insync.replicas` later)

**High-level takeaway for now:** replication + ISR is *why* Kafka can lose a broker
and keep running without losing your data. The knobs come in the intermediate days.

Notes:
Keep this conceptual in the intro. Module 11 (Reliability) is where acks, min.insync.replicas, and the durability trade-offs get hands-on.

---

## Retention — How Long Events Live

Kafka is storage, not just a pipe — but not infinite storage. **Retention** decides
how long events stay before deletion.

- **Time-based** — keep events for N hours/days (e.g. `retention.ms=7 days`)
- **Size-based** — keep up to N bytes per partition, then drop the oldest
- Retention is **per topic** — a short-lived clickstream and a long-lived audit log can differ
- Consumers can re-read anything still within retention — that's **replay**

```
   ┌────┬────┬────┬────┬────┬────┐
   │ e0 │ e1 │ e2 │ e3 │ e4 │ e5 │
   └────┴────┴────┴────┴────┴────┘
     ▲──── past retention ────▲
     deleted                  still readable (replayable)
```

---

## Log Compaction — Keep the Latest per Key

A second retention mode: instead of deleting by age, **keep the latest value for
each key**.

- For **keyed** topics that represent *current state* — "the latest address for each user"
- Compaction removes **superseded** values, keeping at least the most recent per key
- Perfect for **changelog / state** topics; a new consumer can rebuild current state from the log

```
before compaction:  (user-1,A) (user-2,X) (user-1,B) (user-2,Y) (user-1,C)
after  compaction:                        (user-2,Y)            (user-1,C)
                    only the latest value per key survives
```

Notes:
Compaction underpins Kafka Streams' KTables and Connect's offset storage. Introduce the idea here; it returns in the stream-processing module.

---

## Putting It Together

- A **topic** is an append-only **log**, split into **partitions** for scale
- Every event has an **offset**; a message key decides its **partition** and preserves per-key order
- **Producers** append, **consumers** read, **brokers** store — a **consumer group** shares the work
- **Partition count** sets the parallelism ceiling; **different groups** each read the whole topic
- **Replication + ISR** keep data safe across broker failures
- **Retention** and **compaction** decide how long — or in what form — events live

Next module: how these pieces are arranged into a **Kafka 4 cluster**, and how
**KRaft** replaced ZooKeeper.

---

## Summary

- **topic = log**, **partition = ordered shard**, **offset = position**
- **key → partition**, and ordering holds **within a partition**
- **consumer group** scales reads; **partitions cap** the parallelism
- **replication + ISR** provide fault tolerance
- **retention** (time/size) and **compaction** (latest-per-key) govern the log's lifetime