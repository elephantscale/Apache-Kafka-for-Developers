# Confluent Platform for Developers

© Elephant Scale, 9 August 2026

- **Format:** 3 days
- **Level:** Intermediate — assumes Kafka fundamentals
- **Platform:** Confluent Platform 7.9 (self-managed), KRaft mode — ZooKeeper-free
- **Teaching day:** 08:30 – 16:30, with an hour for lunch
- **Hands-on:** more than half the course at the keyboard — Day 3 entirely so
- **Structure:** 9 modules taught across days 1–2, then a full day building it yourself
- **Continuity:** one application built across all three days

---

## Description

Apache Kafka is the protocol. **Confluent Platform is the product enterprises actually
run** — and the distance between the two is where a great deal of developer time is spent.
This course closes it.

Every module is delivered on a real Confluent Platform cluster: `cp-server` brokers,
Control Center, Confluent Schema Registry, ksqlDB, Kafka Connect with Confluent Hub, and
REST Proxy. Participants drive the platform through **Control Center and the
`confluent` CLI** — the same interfaces they use at their desks. The Apache CLI tools
(`kafka-topics.sh` and friends) appear only where they are still the right tool, and are
named as such.

The modules that cover client development — producers, consumers, Connect — are taught
against Confluent tooling throughout, so that a mis-tuned producer is diagnosed in Control
Center rather than in a log file.

## A note on this delivery

**This course is the result of feedback and a course correction.** The earlier
Intermediate delivery was paused because its content sat closer to Apache Kafka than to
the Confluent Platform this team actually runs. That was a fair call, and it is the kind
of feedback we would far rather receive than not.

We are grateful for the opportunity to make the correction and to continue the delivery
with the same group. The pivot is substantive rather than cosmetic — the brokers are
`cp-server` rather than Apache images, Control Center is the primary interface from the
first exercise onward, governance moves to the broker where it can actually be enforced,
and six of the nine modules cover capability that has no Apache Kafka equivalent at all.
The sections that follow set out precisely which, and where participants meet each one,
so the change can be checked rather than taken on trust.

## The course at a glance

| Day | Module | Focus | Format | Time | Confluent-only subject |
|---|---|---|---|---|---|
| **1** | **1** | What Confluent Platform adds to Apache Kafka | Demonstrated | 45 min | Yes |
| **1** | **2** | Producers on Confluent Platform | Demonstrated | 60 min | Confluent tooling |
| **1** | **3** | Consumers, groups, and lag in Control Center | **Hands-on** | 100 min | Confluent tooling |
| **1** | **4** | Confluent Schema Registry in depth | Demonstrated | 60 min | Yes |
| **1** | **5** | Data contracts and broker-side enforcement | **Hands-on** | 95 min | Yes |
| **2** | **6** | Kafka Connect the Confluent way | Demonstrated | 65 min | Confluent tooling |
| **2** | **7** | ksqlDB | **Hands-on** | 100 min | Yes |
| **2** | **8** | RBAC and secure development | Demonstrated | 55 min | Yes |
| **2** | **9** | Platform features that change application design | Demonstrated | 60 min | Yes |
| **2** | — | **Capstone** — the whole application under failure | **Hands-on** | 80 min | Yes |

**All nine modules are delivered across days 1 and 2.** Nothing is cut; what changes
is who is at the keyboard. Three modules and the capstone are full participant labs; the
remaining six are led from the front, with the instructor driving and the room reading
the result together.

The two figures below look inconsistent and are not: **14 participant exercises against 23
demonstrations across days 1 and 2, yet more than half the course is spent at the
keyboard.** A lab runs far longer than a demonstration — 375 of those two days' 720
teaching minutes are hands-on, because the hands-on blocks are the long ones — and **Day 3
is hands-on from start to finish.**

**Why these four are hands-on.** They are the moments that do not survive being watched:

- **Module 3** — moving the commit before the processing, and seeing Control Center report
  **LAG = 0** while your own application quietly drops records
- **Module 5** — producing a record and having the **broker itself** reject it
- **Module 7** — writing streaming SQL, which is only convincing once you have typed it
- **Capstone** — killing a broker under your own running pipeline and proving nothing was lost

Six modules are demonstrated because watching them costs little: the concepts land from a
clear walkthrough, and the configuration is rarely a developer's to write anyway.

## What is Confluent-specific, and where participants touch it

The table below is the course's answer to "is this just Apache Kafka?" — every row is a
capability the Apache distribution does not ship, mapped to where it appears in the
course. Rows marked *(demonstrated)* are shown from the front; the rest sit inside
exercises participants run or follow along with.

| Confluent Platform capability | In Apache Kafka | Where it appears |
|---|---|---|
| **Control Center** — cluster, topic, consumer-lag and end-to-end latency UI | Nothing equivalent | 1.1, 1.2, 1.4, 2.2, 3.2, 3.3, 6.1, 6.3, 9.4 |
| **`confluent` CLI** | Nothing equivalent | 1.2, Module 1 *Go further* |
| **Confluent Monitoring Interceptors** — per-client end-to-end latency | Nothing equivalent | 2.2, 2.3 |
| **Confluent Schema Registry** — subjects, compatibility, references | Nothing equivalent | 4.1 – 4.4 |
| **Broker-side schema validation** — the broker rejects unregistered data | Nothing equivalent | 5.1, 5.2 |
| **Data Contracts** — schema metadata, domain and migration rules | Nothing equivalent | Module 5 *Go further* |
| **Client-Side Field-Level Encryption (CSFLE)** | Nothing equivalent | 5.3, 5.4 |
| **Confluent Hub** — supported, versioned connectors | Community connectors only, unsupported | 6.1, 6.2 |
| **ksqlDB** — streaming SQL, materialized views, pull queries | Kafka Streams (Java library, no SQL) | 7.1 – 7.4 |
| **RBAC via the Metadata Service** — roles scoped to topics, groups, subjects, connectors | ACLs only — no roles, no principals service | Module 8 *(demonstrated)* |
| **Cluster Linking** — byte-for-byte topic mirroring, offset-preserving | MirrorMaker 2, with offset translation caveats | 9.2 *(demonstrated)* |
| **Self-Balancing Clusters** | Manual partition reassignment | Module 9 |
| **REST Proxy** — produce and consume over HTTP | Nothing equivalent | 9.3 |
| **Tiered Storage** | KIP-405 exists in Apache 3.9+; Confluent's is older, with broader backend support | 9.1 *(demonstrated)* |

> Stated plainly for the technical reviewer: **Tiered Storage and Connect are the two rows
> with a genuine Apache counterpart**, and they are labeled as such above. Every other row
> is capability that arrives only with Confluent Platform.

## How the course runs

Modules come in two shapes. Both end with something broken on purpose, because the failure
is usually where the understanding is.

**Hands-on module — ~100 min** (Modules 3, 5, 7, and the capstone)

| Beat | Time | What happens |
|---|---|---|
| **Probe** | ~15 min | A hands-on task participants cannot yet complete. They hit the wall first. |
| **Explain** | ~30 min | The concept — delivered as the answer to what they just ran into. |
| **Build** | ~40 min | The substantial lab. Working code or working configuration. |
| **Break it** | ~15 min | Deliberately break what they built and read the failure. |

The *Probe* is what makes these stick. Participants measure a slow producer before anyone
says the word `linger.ms`; they watch a consumer group leave two members idle before
anyone explains partition assignment. The explanation lands because they already have the
question.

**Demonstrated module — ~60 min** (Modules 1, 2, 4, 6, 8, 9)

| Beat | Time | What happens |
|---|---|---|
| **Explain** | ~25 min | The concept, with the platform on screen rather than slides. |
| **Show** | ~25 min | The instructor drives it live — the same lab, run once, at the front. |
| **Break it** | ~10 min | The failure induced deliberately, and read together. |

Participants follow with the environment open in front of them. Every demonstrated
exercise is written up in full in the lab guide they keep, so anyone can run it afterwards
— and **Day 3** exists precisely to do that with an instructor in the room.

**The teaching day is 08:30 to 16:30 with an hour for lunch** — about seven hours, of
which roughly six are module time once breaks are taken out. Timings are a rhythm, not a
stopwatch; the instructor moves the boundary when a room needs longer on something.

**One application, start to finish.** Rather than nine disconnected labs, the course
builds a single event pipeline incrementally. Each module adds a stage to something that
stays running, whether the room builds that stage or watches it built, so the capstone
hardens a system they have followed since the first morning rather than starting
something new:

| After module | What the application can do |
|---|---|
| **1** | Topics exist; the cluster can be inspected and driven from Control Center |
| **2** | A tuned producer writes events, visible in Control Center with end-to-end latency |
| **3** | A consumer group reads them with correct commit semantics, and its lag is readable |
| **4** | Every event carries a registered schema that can be evolved without breaking readers |
| **5** | The broker refuses off-contract data, and a PII field is encrypted in flight and at rest |
| **6** | A database source feeds the pipeline, an object-store sink drains it, poison records go to a dead letter queue |
| **7** | ksqlDB enriches and aggregates the stream into a materialized view an application can query |
| **8** | Its access requirements are written up as a role-binding request the platform team could act on |
| **9** | It survives a broker loss with no data lost, replays from an earlier offset to rebuild, and is reachable over HTTP |

By the final afternoon they have an application, not a folder of snippets.

**Uneven pace is planned for.** Every lab has a *Go further* stretch task. Fast
participants extend; everyone else moves on at the same time. No one waits and no one is
stranded.

## Audience

Software developers and data engineers building applications against a self-managed
Confluent Platform cluster. Not a course for platform operators, though developers will
finish able to read what the operations team sees.

## Prerequisites

- Practical Java experience (all labs are Java 17 + Maven; a Python reference appendix is provided)
- Comfort with the Linux command line
- Kafka fundamentals: topics, partitions, offsets, producers, consumers, consumer groups
  (the 1-day *Introduction* course, or equivalent experience)

Because this group has already covered the fundamentals with us, the course opens at
working pace rather than re-teaching them. Where earlier material is needed it is recalled
in a sentence, not repeated as a module.

## Objectives

By the end of the course, participants will be able to:

| # | Objective | Covered in |
|---|---|---|
| 1 | Name every component of Confluent Platform, what it costs, and when to reach for it | Module 1 |
| 2 | Operate the platform as a developer through **Control Center** and the **`confluent` CLI** | Modules 1, 3 |
| 3 | Write, tune, and diagnose producers and consumers using **Confluent Monitoring Interceptors** and Control Center's end-to-end latency and consumer-lag views | Modules 2, 3 |
| 4 | Choose and configure delivery guarantees, including exactly-once with transactions | Modules 2, 3 |
| 5 | Design and evolve **data contracts** in Confluent Schema Registry — compatibility modes, schema references, rules, and field-level encryption | Modules 4, 5 |
| 6 | Enforce those contracts **at the broker** with schema validation, so bad data cannot land | Module 5 |
| 7 | Build pipelines with Kafka Connect and Confluent Hub connectors, managed from Control Center | Module 6 |
| 8 | Write stream processing in **ksqlDB** — streams, tables, push and pull queries, materialized views | Module 7 |
| 9 | Work within **RBAC**: understand principals, role bindings, and what to request from the platform team | Module 8 |
| 10 | Exploit **Tiered Storage** and **Cluster Linking** as application design options, not just operational features | Module 9 |

Every module maps to at least one objective, and every objective is exercised at the
keyboard rather than only described.

---

# Day 1 — The Platform, the Clients, and the Data Contract

*Modules 1–5. Two jobs in one day. The morning gets everyone productive inside the
platform itself — Control Center and the `confluent` CLI rather than shell scripts — and
through the client material that carries over from general Kafka work, taught here against
Confluent tooling: a producer tuned by reading end-to-end latency in Control Center, a
broken consumer diagnosed from a lag chart rather than a log file. The afternoon turns to
what most sharply separates Confluent Platform from Apache Kafka — agreeing the shape of
the data, then enforcing that agreement at the broker, where no client can bypass it.*

**Hands-on today:** Module 3 (consumers and lag) and Module 5 (broker-side enforcement).

**By the end of Day 1** the pipeline is running and visible in Control Center, every event
carries a registered schema that can evolve without breaking readers, the broker itself
rejects anything off-contract, and a PII field is encrypted.

## Module 1 — What Confluent Platform Adds to Apache Kafka

**Topics covered**

### 1.A The component map

- `cp-server` versus the Apache broker — what the Confluent broker adds and why it matters
- Control Center, Schema Registry, Connect, ksqlDB, REST Proxy, Metadata Service
- Which components are mandatory, which are optional, and what each costs to run
- How the pieces fit together in a typical enterprise deployment

### 1.B Licensing tiers, and what your subscription entitles you to

- Apache 2.0, Confluent Community License, Confluent Enterprise — what falls where
- **Which capabilities your Confluent subscription unlocks**, so developers design against
  features they are actually licensed to use rather than avoiding them out of caution
- How to tell, from the documentation alone, which tier a feature belongs to
- What a developer should confirm with the platform team before building on a feature

### 1.C Confluent Platform versus Confluent Cloud

- Self-managed and fully managed side by side
- What changes for application code, and what does not
- What transfers if the organization later moves to Cloud

### 1.D Reading Confluent's documentation correctly

- Version mapping: CP 7.9 ≈ Kafka 3.9, CP 8.x ≈ Kafka 4.x
- Telling Platform docs from Cloud docs — the most common source of wasted time
- Spotting whether a documented feature is Community or Enterprise before relying on it

**Demonstrated** — the instructor drives this at the front; participants follow with the
environment open. Every step is written up in full in the lab guide, and Day 3 is time to
run it yourself.

- **1.1 Probe** — Given a running cluster and nothing else: how many brokers, where do
  schemas live, who is consuming right now? Find out using only Control Center.
- **1.2 Shown** — Create topics, inspect partitions and replicas, produce, and browse
  messages from Control Center and the `confluent` CLI. No shell scripts.
- **1.3 Compare** — Do the same tasks with `kafka-topics.sh`. Decide, as a group, when each
  tool is the right one.
- **1.4 Break it** — Delete a topic that Control Center shows as in use. Read what happens
  to the consumer.
- *Go further* — Script the whole setup with `confluent` CLI so it is repeatable.

## Module 2 — Producers on Confluent Platform

**Topics covered**

### 2.A Producer architecture

- The record accumulator, batching, and the sender thread
- What actually happens between `send()` returning and the record reaching a broker
- Why `send()` is asynchronous, and what that means for error handling
- Callbacks, futures, and where exceptions really surface

### 2.B Throughput and latency

- `linger.ms` and `batch.size` — the two knobs that dominate throughput
- `compression.type` — the trade between CPU, network, and broker storage
- Finding the knee in the curve rather than copying values from a blog post
- Buffer exhaustion: `buffer.memory`, `max.block.ms`, and what backpressure looks like

### 2.C Durability and ordering

- `acks=0`, `1`, and `all`, and what each one actually risks
- `min.insync.replicas` as the other half of the `acks=all` contract
- Idempotence — default since Kafka 3.0 — and the guarantee it provides
- `max.in.flight.requests.per.connection`, retries, and preserving order
- Transactions and exactly-once semantics: when they are worth the cost

### 2.D Partitioning

- How keys map to partitions, and murmur2 hashing
- Choosing a key: ordering guarantees versus partition skew
- Custom partitioners — and an honest look at when they are worth writing

### 2.E Observability with Confluent tooling

- Confluent Monitoring Interceptors — two lines of configuration
- Reading producer end-to-end latency in Control Center
- Diagnosing a mis-tuned producer from the UI rather than from log files

**Demonstrated** — the instructor drives this at the front; participants follow with the
environment open. Every step is written up in full in the lab guide, and Day 3 is time to
run it yourself.

- **2.1 Probe** — Run the supplied producer. Measure its throughput. Write your number on
  the whiteboard next to everyone else's. Nobody has said `linger.ms` yet.
- **2.2 Shown** — Add the Confluent Monitoring Interceptor — two lines of config — and watch
  your producer appear in Control Center with end-to-end latency.
- **2.3 Shown** — Sweep `linger.ms`, `batch.size`, and `compression.type`. Chart throughput
  against latency. Find the knee in the curve.
- **2.4 Break it** — Set `acks=0`, kill a broker mid-run, and count exactly what was lost.
  Then set `acks=all` and repeat.
- *Go further* — Write a custom partitioner, then argue whether it was worth it.

## Module 3 — Consumers, Groups, and Lag in Control Center

**Topics covered**

### 3.A The poll loop

- What `poll()` really does — fetching, heartbeating, and rebalance participation
- `max.poll.records` and `max.poll.interval.ms`
- The stop-the-world failure: slow processing evicts the consumer, which triggers a
  rebalance, which makes processing slower still
- Separating processing from polling when work is genuinely slow

### 3.B Offset commits and delivery semantics

- Auto-commit versus manual commit, and why auto-commit surprises people
- `commitSync` and `commitAsync` — cost, blocking, and failure behavior
- **Commit ordering is the whole game**: committing before processing gives at-most-once,
  after processing gives at-least-once
- Why exactly-once needs more than a commit strategy
- Storing offsets outside Kafka, and when that is justified

### 3.C Where a consumer starts

- `auto.offset.reset` — `earliest`, `latest`, `none`
- Why it is ignored whenever a committed offset already exists, which is the single most
  common misunderstanding in this area
- Seeking deliberately: replaying a window of history on demand

### 3.D Groups and rebalancing

- Group membership, partition assignment, and the partition-count ceiling
- Assignment strategies: eager, cooperative-sticky, and KIP-848
- What a rebalance costs in a live application, and how to reduce it
- Static membership for rolling restarts

### 3.E Lag in Control Center

- Reading consumer lag, and what a healthy lag curve looks like under load
- Why **lag alone can lie** — a consumer that commits early reports zero lag while
  dropping records
- Consumer-group views the operations team will be watching

**Hands-on** — participants build this themselves.

- **3.1 Probe** — Start five consumers in one group against a 3-partition topic. Two do
  nothing. Work out why before it is explained.
- **3.2 Build** — Write a consumer that commits *after* processing. Watch lag rise and fall
  in Control Center under load.
- **3.3 Break it** — Move the commit *before* the processing. Watch Control Center report
  **LAG = 0** while your application has handled a fraction of the records. This is the
  single most important slide of the day, and it is not a slide.
- **3.4 Build** — Seek to a chosen offset and replay a window of history on demand.
- *Go further* — Introduce the Confluent Parallel Consumer for key-level concurrency beyond
  the partition count.

---

## Module 4 — Confluent Schema Registry in Depth

**Topics covered**

### 4.A The wire format and schema identity

- Subjects, versions, and schema IDs — three things people routinely conflate
- The magic byte and the schema ID carried in every record
- What a consumer does when it meets an ID it has never seen
- Why a raw `kafka-console-consumer` shows garbage on a schema-backed topic

### 4.B Subject naming strategies

- TopicName, RecordName, and TopicRecordName
- The design consequence of each: one schema per topic, or many
- Which strategy to choose when a topic legitimately carries several event types

### 4.C Compatibility modes

- BACKWARD, FORWARD, FULL, and their `_TRANSITIVE` forms
- **Choosing the mode from who upgrades first** — consumers or producers — rather than
  picking a default and hoping
- What each mode permits and forbids: adding fields, removing fields, defaults
- Compatibility checked at registration time, before bad data exists

### 4.D Avro, Protobuf, and JSON Schema

- The three formats side by side: tooling, code generation, payload size, readability
- Where each is the sensible default
- Migration realities when an organization already has one of them

### 4.E Composition and portability

- Schema references — composing schemas instead of copying fields
- Schema Linking: moving schemas between development, test, and production
- Keeping registries consistent across environments

**Demonstrated** — the instructor drives this at the front; participants follow with the
environment open. Every step is written up in full in the lab guide, and Day 3 is time to
run it yourself.

- **4.1 Probe** — Dump the raw bytes of a record. Find the magic byte and the schema ID.
  Resolve that ID against the registry by hand.
- **4.2 Shown** — Register v1, run a consumer, evolve to v2 under BACKWARD compatibility,
  and confirm the old consumer survives untouched.
- **4.3 Break it** — Attempt an incompatible change and read the rejection. Switch the
  subject to FORWARD and watch which changes are now legal and which are not.
- **4.4 Shown** — Manage schemas from Control Center and from the REST API; compare.
- *Go further* — Compose two schemas with a schema reference instead of copying fields.

## Module 5 — Data Contracts and Broker-Side Enforcement

**Topics covered**

### 5.A Broker-side schema validation

- A `cp-server` capability with no Apache Kafka equivalent: the **broker** refuses data
  that does not carry a registered schema
- Enabling it per topic with `confluent.value.schema.validation`
- Key validation and value validation as separate decisions
- Exactly what the producer sees when the broker rejects a record, and how to handle it
- Why a client-side serializer is not enough: any process with network access can bypass it

### 5.B Data Contracts

- A contract as more than a schema: metadata, domain rules, and migration rules
- Domain rules that reject records which are schema-valid but semantically wrong
- Migration rules for evolving data without breaking existing consumers
- Ownership and documentation travelling with the schema

### 5.C Client-Side Field-Level Encryption (CSFLE)

- Encrypting individual fields — PII — rather than the whole payload
- What an unauthorized reader sees, including in the Control Center message browser
- Key management, and what happens to a consumer without the key
- Where CSFLE fits alongside TLS and disk encryption, which solve different problems

### 5.D Where enforcement belongs

- Client, broker, or both — the trade-offs stated plainly
- Performance and operational cost of broker-side validation
- Designing a rollout that does not break producers already in flight

**Hands-on** — participants build this themselves.

- **5.1 Probe** — Produce a well-formed but unregistered record to a schema-backed topic
  using a plain `StringSerializer`. It lands. Everyone sees that nothing stopped it.
- **5.2 Build** — Enable `confluent.value.schema.validation`. Run the *same* producer. The
  **broker** now rejects it. Handle the rejection in code.
- **5.3 Build** — Encrypt a PII field with CSFLE. Confirm it is unreadable in the Control
  Center message browser while the authorized consumer still reads it cleanly.
- **5.4 Break it** — Withhold the key and watch the consumer fail. Decide what your
  application should do about that.
- *Go further* — Add a domain rule that rejects a semantically invalid but
  schema-valid record.

# Day 2 — Pipelines, Processing, and Hardening

*Modules 6–9, then the capstone. Turning a pipeline into a system. Data moves in and out
through Connect, processing moves into the stream rather than a nightly batch, the security
model is made explicit, and the platform features that genuinely change how a system is
designed — effectively unlimited retention, mirrored clusters, HTTP access — are examined
for what they make possible rather than as operational trivia. The day closes by breaking
the whole thing on purpose.*

**Hands-on today:** Module 7 (ksqlDB) and the capstone.

**By the end of Day 2** data flows in from a database and out to object storage with poison
records routed aside, ksqlDB enriches and aggregates the stream into a materialized view
queried from Java, participants can write an access request their platform team could act
on, and they have watched their own application survive a broker failure with no records
lost.

## Module 6 — Kafka Connect the Confluent Way

**Topics covered**

### 6.A The Connect runtime

- Workers, connectors, and tasks — what actually runs where
- Distributed mode and the three internal topics (`configs`, `offsets`, `status`)
- Scaling by task, and why a connector's parallelism has a ceiling
- Where Connect state lives, and what survives a worker restart

### 6.B Sourcing connectors from Confluent Hub

- Community, commercially-licensed, and Confluent-supported connectors
- **How to tell which one you are installing** before it reaches production
- Versioning and upgrade practice
- Installing into an image versus installing at container start

### 6.C Converters and Schema Registry

- Key and value converters, and how they couple Connect to Schema Registry
- Avro, Protobuf, JSON Schema, String, and ByteArray converters
- **Converter mismatch — the single most common Connect failure in production**
- Reading a failed task's stack trace and working back to the misconfigured converter

### 6.D Transformations

- Single Message Transforms: reshaping records in flight without touching the producer
- Chaining transforms, and the point at which a stream processor is the better answer
- Predicates for applying a transform selectively

### 6.E Failure handling

- Dead letter queues: routing poison records instead of stopping the connector
- The failure headers on a DLQ record, and what they tell you
- `errors.tolerance` and deciding what a pipeline should survive

### 6.F Security for connectors

- RBAC for Connect principals — a connector is a client and needs its own identity
- Scoping a connector's permissions to the topics it genuinely needs
- Keeping credentials out of connector configuration

**Demonstrated** — the instructor drives this at the front; participants follow with the
environment open. Every step is written up in full in the lab guide, and Day 3 is time to
run it yourself.

- **6.1 Shown** — Deploy a JDBC source connector from Postgres, configured and started
  entirely in Control Center.
- **6.2 Shown** — Add an S3 sink. Verify the objects landed and inspect their format.
- **6.3 Break it** — Introduce a converter mismatch, the single most common Connect
  failure in production. Diagnose it from the failed task's trace in Control Center, then
  fix it.
- **6.4 Break it** — Send a poison record. Route it to a DLQ. Consume the DLQ and read the
  failure headers.
- *Go further* — Chain two SMTs to reshape records in flight, without touching the producer.

---

## Module 7 — ksqlDB

**Topics covered**

### 7.A Streams and tables

- The stream–table duality, and why it is the whole idea rather than a detail
- A stream as a log of events; a table as the current state that log implies
- `CREATE STREAM` and `CREATE TABLE` over existing topics
- Why the choice between them is a modelling decision, not a syntax preference

### 7.B Push and pull queries

- Push queries (`EMIT CHANGES`) — a query that never ends
- Pull queries — point-in-time lookups against materialized state
- Which one belongs in an application, and which in a console
- Serving an application from a pull query over the REST API

### 7.C Joins

- Stream–stream joins and the windowing they require
- Stream–table joins for enrichment — the most common production pattern
- Table–table joins
- Co-partitioning: the requirement that quietly breaks joins in practice

### 7.D Windowing and aggregation

- Tumbling, hopping, and session windows, and what each is for
- Late-arriving data and grace periods
- Aggregations into materialized views

### 7.E Guarantees and operations

- Exactly-once processing in ksqlDB, and proving it under a forced restart
- What ksqlDB state costs, and where it is stored
- Managing queries from Control Center

### 7.F Choosing between ksqlDB, Kafka Streams, and Flink

- SQL versus a JVM library: expressiveness against operational control
- Where Confluent is heading with Apache Flink
- An honest comparison, including what SQL gives up

**Hands-on** — participants build this themselves.

- **7.1 Probe** — Re-express Day 1's Java enrichment pipeline in ksqlDB. Count the lines of
  each. Then discuss honestly what you gave up.
- **7.2 Build** — Stream-table join to enrich the running application's events.
- **7.3 Build** — Windowed aggregation into a materialized view. Query it with a pull query
  while a push query streams alongside.
- **7.4 Build** — Query that materialized view from Java over the ksqlDB REST API — the
  pattern for serving an application from a stream.
- *Go further* — Make the same query exactly-once and prove it under a forced restart.

## Module 8 — RBAC and Secure Development

**Topics covered**

### 8.A How authorization works in Confluent Platform

- The Metadata Service (MDS) and what it adds over Apache Kafka ACLs
- Authentication versus authorization — routinely confused, and different problems
- Where authorization decisions are made and how they are cached

### 8.B Principals and service accounts

- Users, service accounts, and choosing an identity for an application
- Why applications should not run as a human's principal
- What a developer requests, and what the platform team provisions

### 8.C Roles and scopes

- The developer-relevant roles: `DeveloperRead`, `DeveloperWrite`, `DeveloperManage`,
  `ResourceOwner`
- Scoping a binding to topics, consumer groups, Schema Registry subjects, and connectors
- Prefix and literal resource patterns
- **Finding true least privilege empirically** — narrowing a binding until it breaks

### 8.D Diagnosing authorization failures

- Reading the exception and working back to the missing permission
- Distinguishing a permissions failure from a connectivity or authentication failure
- Consumer groups and subjects as separately-secured resources people forget

### 8.E Secrets and client configuration

- Keeping credentials out of source control and configuration files
- Where secrets belong in a deployed application
- Writing an access request the platform team can act on without a follow-up conversation

**Demonstrated, not built**

RBAC requires the Metadata Service and an enterprise identity source — infrastructure
that belongs to the platform team, not to a training VM. This module is therefore
instructor-led at the keyboard, projected, with participants following the reasoning
rather than provisioning their own. That matches how developers meet RBAC in practice:
they read failures and request bindings; they do not create them.

- **8.1 Demonstration** — The working application run as an unprivileged principal. The
  exact exception, read together, and the group works out which permission is missing.
- **8.2 Demonstration** — The role binding that fixes it, created and applied live. Re-run.
  It works.
- **8.3 Demonstration** — The binding narrowed one scope at a time until it breaks again,
  finding true minimum privilege empirically in front of the room.
- **8.4 Discussion** — Scoping the Connect connector's own principal from Module 6.
- **Participant exercise** — Write the access request you would actually send your platform
  team: exact resource names, roles, and scopes, for the application you have been building
  all week. This is the artefact a developer really produces, and it is graded by whether
  the platform team could act on it without a follow-up conversation.

## Module 9 — Platform Features That Change Application Design

**Topics covered**

### 9.A Tiered Storage

- Hot local storage and cold object storage behind one topic
- What effectively-unlimited retention changes about application design — replay,
  bootstrapping a new consumer, and audit
- The cold-read latency penalty, measured rather than assumed
- When retention becomes a design choice instead of a cost constraint

### 9.B Cluster Linking

- Byte-for-byte topic mirroring with offsets preserved
- How that differs from MirrorMaker 2, and why offset translation matters
- Consuming from a linked topic, and the failover contract an application must honor
- Use cases: disaster recovery, data locality, and migration

### 9.C Self-Balancing Clusters

- Automatic partition rebalancing versus manual reassignment plans
- What a developer notices while rebalancing is under way

### 9.D REST Proxy

- Producing and consuming over HTTP for callers that cannot host a Java client
- The integration path for non-JVM languages and legacy systems
- Where the REST Proxy is the right answer, and where it is a workaround

### 9.E Operations, seen from the application side

- Health+ and what the operations team actually watches
- Quotas and throttling — what a throttled client experiences, and how to behave well
- Reading the signals that predict trouble before an incident

**Demonstrated, then the capstone**

The platform features are shown from the front — none of them is a developer's to
configure. The capstone that closes the course is the opposite: everyone at the keyboard,
on the application they have followed since the first morning.

- **9.1 Demonstration** — Replay from a Tiered Storage offset older than local retention,
  against a cluster with real history behind it. The cold-read latency penalty measured
  live, and the room decides together whether they would accept it in their own design.
- **9.2 Demonstration** — Cluster Linking shown against a second cluster, which a single
  training VM cannot host. Consuming from a linked topic, and the failover contract an
  application must honor, walked through together.
- **9.3 Demonstration** — Producing and consuming over HTTP through the REST Proxy with
  nothing but `curl` — the integration path for non-JVM callers.
- **9.4 Capstone — hands-on** — The full application under failure: schema-validated ingest, ksqlDB
  enrichment, Connect sink. Kill a broker mid-run and **prove zero loss from Control
  Center**. Then replay the stream from an earlier offset and watch it rebuild.
- *Go further* — Add a quota, saturate it, and observe what your client does when throttled.

---

# Day 3 — Build It Yourself

Days 1 and 2 cover all nine modules, but six of them are demonstrated rather than built.
**Day 3 is where the room runs them.**

It is not new material and it needs no new preparation. Every demonstrated exercise is
written up in full in the lab guide participants keep, and every module carries a *Go
further* stretch task. Day 3 is instructor-supported time to work through them on
your own cluster, with someone in the room when something does not behave.

| The day is structured around | Drawn from |
|---|---|
| **Driving the platform yourself** — Control Center and the `confluent` CLI, end to end | Module 1 |
| **Tuning a producer** — sweep `linger.ms`, `batch.size`, compression; find the knee in the curve | Module 2 |
| **Evolving a schema** — register v1, evolve to v2 under BACKWARD, break it under FORWARD | Module 4 |
| **Building the Connect pipeline** — JDBC source, S3 sink, a converter mismatch to diagnose, a poison record to route | Module 6 |
| **The *Go further* tasks** — custom partitioner, schema references, chained SMTs, exactly-once under restart, quota throttling | All modules |

**How the day adapts.** Teams who will be writing this code themselves within weeks work
straight through the exercises. Rooms with mixed experience use it as a second pass, with
the instructor pairing on whatever stalled earlier in the week. Where a group would rather
go forward than back, any of the add-on modules below can take its place.

## Add-On Modules (substitutions, or a further day)

Any of these can replace part of Day 3, or extend the course to a fourth day.

| Module | Fits when |
|---|---|
| **Kafka Streams in Java** | Teams building stateful services in the JVM rather than SQL |
| **Flink SQL on Confluent** | Teams tracking Confluent's direction beyond ksqlDB |
| **Confluent Cloud** | Teams evaluating a managed path — Stream Governance, Catalog, Lineage |
| **Migrating from OSS Kafka to CP** | Sites still running Apache clusters alongside |
| **Confluent for Kubernetes (CFK)** | Sites deploying Confluent Platform via the operator |
| **Performance tuning workshop** | High-volume fan-out, partition sizing, consumer scaling |

---

## Lab Environment

A single Docker Compose stack per participant, on a provided VM. The full platform runs
on each participant's own machine — there is no shared cluster to queue for, and no lab
is blocked by another participant's mistake.

| Service | Image | Notes |
|---|---|---|
| Brokers ×3 | `confluentinc/cp-server:7.9.9` | KRaft mode, combined broker+controller |
| Control Center | `confluentinc/cp-enterprise-control-center:7.9.9` | Primary UI for the whole course |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.9` | |
| ksqlDB Server + CLI | `confluentinc/cp-ksqldb-server:7.9.9`, `cp-ksqldb-cli:7.9.9` | Day 2 |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.9.9` | JDBC + S3 from Confluent Hub |
| REST Proxy | `confluentinc/cp-kafka-rest:7.9.9` | Day 2 |
| Postgres, MinIO | `postgres:16`, `minio/minio` | Connect endpoints, not Kafka components |

**VM sizing — 4 vCPU, 16 GB RAM, 40 GB free disk per participant.** These are measured
numbers, not estimates: the Day 1 core alone (three brokers, Schema Registry, Control
Center) holds 4.5 GB resident, and Control Center accounts for the largest single share.
Day 2 adds Connect, ksqlDB, and REST Proxy on top, alongside a Maven build and an IDE.
**A 12 GB VM is not sufficient** — Control Center starts on less and then fails partway
through the day under metrics load, which costs classroom time to diagnose.

Services are started by day rather than all at once, so no VM carries the full stack
before it is needed. Day 3 runs the same stack as Day 2 — it needs no additional
provisioning.

The participant VM deliberately does **not** host the Metadata Service, an identity
source, a tiered-storage backend with real history behind it, or a second cluster. Those
are what Module 8 and exercises 9.1 and 9.2 demonstrate, and they run on the instructor's
environment rather than being provisioned sixteen times. Keeping them off the student
image is what holds the VM at 16 GB — and a tiered-storage replay is only meaningful
against a topic with more history than a classroom cluster accumulates in a few days.

**The `confluent` CLI is installed on each VM**, not inside a container — it is one of the
two interfaces this course teaches, alongside Control Center. It drives the cluster through
the Admin REST API that `cp-server` exposes on port 8090, so no Metadata Service is
required for the CLI exercises in Module 1.

Client code is Java 17 + Maven, in a ready-made single Maven project — participants write
lab classes, not build files. A one-command startup script brings the environment up each
morning and verifies it is healthy before class begins.

All course materials — slides, labs, and runnable code — are supplied in a Git repository
that participants keep after the course.
