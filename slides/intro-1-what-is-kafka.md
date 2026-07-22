# Intro 1 — What Is Kafka and Why

Elephant Scale

---

## Agenda

- Real-time streaming vs. batch; the problem Kafka solves
- Event-driven architecture: publish/subscribe vs. request/response
- Kafka use cases and who uses Kafka
- Where Kafka fits: the modern data platform

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

Notes: Anchor this in the students' own systems — ask what runs as a nightly job today and what the business wishes were real-time. Almost everyone has an example.

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

Notes: Emphasize "log," not "queue." A queue forgets a message once it is consumed; Kafka keeps it, which is what enables replay and multiple independent consumers.

---

## Event-Driven Thinking

An **event** is a fact that already happened: *"Order 1234 was placed at 10:02."*

- Immutable — you don't edit the past, you append a new event
- Named in the **past tense** — `OrderPlaced`, `PaymentCaptured`, `ItemShipped`
- Carries just enough data for consumers to react

Event-driven systems are built by **producing** these facts and letting any number
of interested parties **react** — without the producer orchestrating them.

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

Notes: Both patterns coexist in real systems. The point isn't "REST is bad" — it's that fan-out, decoupling, and replay are exactly where request/response gets painful and pub/sub shines.

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

---

## Who Uses Kafka

Kafka runs the real-time backbone at companies operating at enormous scale:

- **LinkedIn** — where Kafka was created; trillions of messages per day
- **Uber** — matching, pricing, and fraud on live event streams
- **Netflix** — streaming telemetry and data pipelines
- **Thousands of enterprises** across banking, retail, telecom, logistics, and IoT

It has become the **de facto standard** for event streaming — which is why fluency
with Kafka is now a core skill for developers, not a specialty.

Notes: The scale numbers matter less than the ubiquity. If they join almost any data-heavy engineering team, Kafka will be somewhere in the architecture.

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

---

## Summary

- Batch answers "what happened yesterday"; **streaming** answers "what's happening now"
- Kafka replaces the **N×M** integration tangle with one durable log in the middle
- It **decouples** producers from consumers and lets consumers **replay** history
- Publish/subscribe adds new consumers with **zero producer changes**
- Kafka is the **de facto standard** for event streaming and the hub of the modern data platform