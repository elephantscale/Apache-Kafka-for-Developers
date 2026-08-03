# Intro 1 — What Is Kafka and Why

Elephant Scale

---

## Agenda

- Real-time streaming vs. batch; the problem Kafka solves
- Where Kafka came from
- Event-driven architecture: publish/subscribe vs. request/response
- Kafka use cases and who uses Kafka
- Where Kafka fits: the modern data platform
- Streaming-first design and common enterprise EDA patterns

Notes:
Before the agenda, run the hook: ask who still has an overnight batch job, then who has been asked for that same thing in real time. That gap IS the course. ~2 min on this slide, but the hook is worth 5.

---

## The World Got Faster Than Batch

For decades, data moved in **batches**: collect all day, run a job at night, see
results tomorrow.

- A bank posts transactions overnight
- A retailer recomputes inventory at 2 AM
- Analytics dashboards are always "yesterday's numbers"

That was fine when the business ran on a daily cycle. It is not fine when:

- A fraud decision has to happen **before** the payment clears
- A rider needs a driver matched **now**
- A dashboard is expected to be live, not 24 hours stale

The world moved to **events that must be reacted to as they happen**. Batch can't do that.

Notes:
Anchor this in the students' own systems — ask what runs as a nightly job today and what the business wishes were real-time. Almost everyone has an example. Write two or three on the board and REUSE THEM ALL WEEK in place of generic orders/payments; it turns the most lecture-heavy hour of the course into a conversation. Budget 5-8 min, most of it theirs.

---

## Batch vs. Streaming

| | Batch | Streaming |
|---|---|---|
| Unit of work | A file / a big query | A single event |
| When it runs | On a schedule (nightly, hourly) | Continuously, as data arrives |
| Latency | Hours | Milliseconds to seconds |
| Data at rest / in motion | At rest (tables, files) | In motion (a flowing log) |
| Question it answers | "What happened yesterday?" | "What is happening right now?" |

Streaming doesn't replace batch everywhere — it adds the ability to act on data
**while it is still fresh**. Kafka is the platform that makes streaming practical
at scale.

Notes:
Read the table quickly — it is reference, not revelation. ~3 min. Better use of the time: ask which row surprises them, or which of their own jobs could move from the left column to the right.

---

## The Problem Kafka Solves: N×M Integration

Without a streaming platform, every system that produces data connects **directly**
to every system that needs it:

```
 Orders  ──┬──► Fraud
           ├──► Email
           ├──► Analytics
           └──► Warehouse

 Payments ─┬──► Fraud
           ├──► Ledger
           └──► Analytics
```

- Every new consumer means new point-to-point plumbing
- Producers must know about — and keep up with — every consumer
- One slow or down consumer can stall the producer
- **N** producers × **M** consumers = an unmaintainable web of integrations

This is the tangle Kafka was built at LinkedIn to untangle.

Notes:
The centre of gravity of this module — 10-12 min, and build it on the whiteboard rather than showing the finished diagram. Draw Orders to Fraud. Fine. Now Orders to Email. Now Payments to Fraud, Payments to Ledger. Then ask: 'we have just been asked to add a recommendation service that needs both — how many teams do I have to talk to?' Let them feel the combinatorics before the term N×M appears.

---

## Where Kafka Came From

- Built at **LinkedIn** around **2010** to untangle exactly the mess on the previous slide —
  every system wired to every other. They needed **one** high-throughput pipeline for activity
  and operational data.
- Created by **Jay Kreps, Neha Narkhede, and Jun Rao**; open-sourced in **2011**, a top-level
  **Apache** project by **2012**.
- **The name:** Kreps named it after the writer **Franz Kafka** — a system "optimized for
  writing" deserved an author's name.
- The three creators left LinkedIn in **2014** to found **Confluent**, still the main
  commercial steward of Kafka.
- Full circle: LinkedIn today runs Kafka at roughly **7 trillion messages a day**.

<img src="../images/Franz_Kafka,_1923.jpg" width="22%"/> &nbsp;
<img src="../images/kafka-metamorphosis-bug.jpg" width="30%"/>

Notes:
Worth 60 seconds — it makes the technology human and gives the N×M slide a real origin. The Metamorphosis joke lands with most rooms: the one book everyone half-remembers from school, and here it is running your payments.

---

## Kafka Decouples Producers From Consumers

Put a durable **log** in the middle. Producers write to it once; consumers read
independently, at their own pace.

```
 Orders  ──►┐                    ┌──► Fraud
 Payments ──┤   ┌───────────┐    ├──► Analytics
 Clicks  ──►├──►│   Kafka    │───►├──► Warehouse
 Sensors ──►┘   └───────────┘    └──► Email
             producers            consumers
```

- Producers don't know or care who reads the data
- Consumers read at their own speed, and can **replay** history
- Add a new consumer without touching any producer
- A slow consumer can't back-pressure the producer — the log absorbs it

**Decoupling** is the core idea. Everything else is detail.

Notes:
The turn, and the emotional payoff of the previous slide — 8-10 min. Tie each bullet to a pain they just felt: no new plumbing, no producer changes, no back-pressure from a slow consumer. The line to land: 'the producer no longer knows who its consumers are, and that ignorance is the feature.'

---

## What Kafka Actually Is

Apache Kafka is a **distributed, fault-tolerant, horizontally scalable streaming
platform** — an append-only, replayable **log** that many producers and consumers
share.

Three things at once:

- **Publish/subscribe** — write events, read events (like a messaging system)
- **Storage** — events are persisted durably and can be re-read for days, weeks, or forever
- **Processing** — a stream of events can be transformed, joined, and aggregated in motion

Because it is durable and replayable, Kafka is not just a message bus that forgets —
it is the **system of record for events in motion**.

Notes:
Emphasize "log," not "queue." A queue forgets a message once it is consumed; Kafka keeps it, which is what enables replay and multiple independent consumers.

---

## Event-Driven Thinking

An **event** is a fact that already happened: *"Order 1234 was placed at 10:02."*

- Immutable — you don't edit the past, you append a new event
- Named in the **past tense** — `OrderPlaced`, `PaymentCaptured`, `ItemShipped`
- Carries just enough data for consumers to react

Event-driven systems are built by **producing** these facts and letting any number
of interested parties **react** — without the producer orchestrating them.

Notes:
~4 min. Make it concrete with their own domain: ask someone to name three events their system would emit, in past tense. Naming things OrderPlaced rather than createOrder is where the mental shift actually happens.

---

## Publish/Subscribe vs. Request/Response

**Request/Response** (the familiar REST call):

- Caller asks a specific service and waits for an answer
- Tight coupling: the caller must know the callee and it must be up *right now*
- Adding a new interested party means changing the caller

**Publish/Subscribe** (the Kafka way):

- Producer publishes an event and moves on — no waiting, no knowledge of subscribers
- Any number of consumers subscribe independently
- Add a new subscriber with **zero changes** to the producer

```
Request/Response          Publish/Subscribe
  A ──► B  (A waits)        A ──► [ topic ] ──► B
                                            └──► C
                                            └──► D  (added later, A unchanged)
```

Notes:
Both patterns coexist in real systems. The point isn't "REST is bad" — it's that fan-out, decoupling, and replay are exactly where request/response gets painful and pub/sub shines.

---

## Common Use Cases

- **Event-driven microservices** — services communicate through events instead of brittle
  direct calls
- **Log & metrics aggregation** — funnel logs, metrics, and traces from many services into
  one pipeline
- **Website / app activity tracking** — clicks, page views, and user actions as a live stream
  (Kafka's original use at LinkedIn)
- **Real-time analytics & dashboards** — compute metrics continuously instead of nightly
- **Data integration / pipelines** — move data between databases, warehouses, search, and
  object storage (via Kafka Connect)
- **Stream processing** — fraud detection, alerting, enrichment, feature computation on data
  in motion
- **ML feature stores** — feed models with fresh, real-time features

Notes:
Don't read the list — 5 min, spent asking which of these they already have and which they wish they had. Their answers tell you where to aim examples for the rest of the week. If the room is quiet, offer your own: 'anyone doing fraud or alerting?'

---

## Who Uses Kafka

Kafka runs the real-time backbone at companies operating at enormous scale:

| | Scale |
|---|---|
| **LinkedIn** — where Kafka was created | ~7 trillion messages/day |
| **Uber** — matching, pricing, fraud | ~1 trillion messages/day |
| **Netflix** — telemetry and data pipelines | trillions of events/day |
| **Cloudflare** — edge logs and analytics | petabytes/day |
| **Walmart** — peak retail events | billions/day |

Plus **thousands of enterprises** across banking, government, retail, telecom, logistics,
and IoT.

It has become the **de facto standard** for event streaming — which is why fluency
with Kafka is now a core skill for developers, not a specialty.

Notes:
The numbers are headroom, not a target — the point is that nothing you build in this course will strain Kafka. Ubiquity matters more than scale: join almost any data-heavy engineering team and Kafka is somewhere in the architecture.

---

## Where Kafka Fits: The Modern Data Platform

Kafka usually sits in the **center**, as the pipe that everything flows through:

```
   Sources                    Kafka                     Sinks / Consumers
 ┌──────────┐                                          ┌──────────────────┐
 │ apps     │──►┐                              ┌──────►│ microservices    │
 │ databases│──►│   ┌────────────────────┐     │──────►│ data warehouse   │
 │ clicks   │──►├──►│  topics (the log)  │────►│──────►│ search index     │
 │ sensors  │──►│   └────────────────────┘     │──────►│ dashboards       │
 └──────────┘   │      ▲            │           │──────►│ object storage   │
                │      │            ▼           │       └──────────────────┘
                │   Connect     Streams / Flink │
                │  (in/out)   (process in motion)│
                └────────────────────────────────┘
```

- **Kafka Connect** moves data in and out without custom code
- **Kafka Streams / Flink** process the stream as it flows
- **Schema Registry** keeps producers and consumers agreeing on data shape

We'll meet each of these in this course.

Notes:
60 seconds. This is a signpost for the week, not a topic: Connect moves data in and out, Streams/Flink process it in motion, Schema Registry keeps everyone agreeing on shape. Each gets its own module and its own lab. Resist previewing them.

---

## Streaming-First Application Design

The architectural shift Kafka enables — where the **event**, not the database row, is the
source of truth.

**Traditional:**
```
App ──► Database ──► Report   (batch, hours later)
```

**Streaming-first:**
```
App ──► Kafka ──┬──► real-time processor ──► immediate action
                ├──► data lake            (durable replay / history)
                ├──► ML pipeline          (continuous features)
                └──► microservices        (event-driven triggers)
```

- One write, many independent readers — each added without touching the producer
- The database becomes *a* consumer of the log, not the single gatekeeper of truth
- History is retained, so a new consumer can be added later and **replay from the beginning**

Notes:
This is the mental shift that's harder than any API. Teams that keep treating the DB as the source of truth and Kafka as "just a queue in between" never get the benefits. Ask the room where their current source of truth lives.

---

## Enterprise Streaming Patterns

Names you'll hear once Kafka is in the architecture — worth recognizing early:

- **Event Sourcing** — store every state change as an event; derive current state by replaying
  the log rather than overwriting a row
- **CQRS** — separate the **write** model (commands) from the **read** model (queries),
  connected by events
- **Saga** — coordinate a long-running transaction across services as a chain of events with
  compensating actions, instead of a distributed lock
- **CDC (Change Data Capture)** — stream a database's changes into Kafka, so existing systems
  become event sources without being rewritten
- **Fan-out** — one event, many independent consumers reacting on their own

Notes:
Don't teach these in depth here — just plant the vocabulary so the terms aren't new when they appear in design discussions. CDC is usually the most immediately relevant: it's how an organization with established databases starts an event-driven journey without a rewrite.

---

## A Developer's Mental Model

For the rest of this course, hold onto this:

- Kafka is a **durable, ordered, replayable log** you publish events to and subscribe from
- Producers and consumers are **decoupled** — that decoupling is the whole point
- Events are **immutable facts**; you append, you don't overwrite
- Everything else — partitions, offsets, consumer groups, delivery guarantees — exists to make
  that log **fast, scalable, and reliable**

Next module: the **core concepts** that make this log work — topics, partitions,
offsets, and consumer groups.

Notes:
~3 min. Ask them to say the one-sentence version back to you before you show the bullets — 'what is Kafka, in one sentence?' If the answer is 'a message queue', you have found the misconception to correct before Module 2 and you have time to do it.

---

## Summary

- Batch answers "what happened yesterday"; **streaming** answers "what's happening now"
- Kafka replaces the **N×M** integration tangle with one durable log in the middle
- It **decouples** producers from consumers and lets consumers **replay** history
- Publish/subscribe adds new consumers with **zero producer changes**
- **Streaming-first** design treats events, not database rows, as the source of truth
- Patterns to recognize: **Event Sourcing, CQRS, Saga, CDC, fan-out**
- Kafka is the **de facto standard** for event streaming and the hub of the modern data platform

Notes:
2 min. If the room remembers one sentence from this module it should be 'Kafka is a durable, replayable log — not a queue that deletes on read.' Say it, then transition: one log serving thousands of clients is the partition story, which is the next module.

