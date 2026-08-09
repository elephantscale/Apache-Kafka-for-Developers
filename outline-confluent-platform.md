# Confluent Platform for Developers

**Format:** 3 days. **Roughly half the class time is spent at the keyboard.**
Optional 4th day (see *Add-On Modules*).
**Level:** Intermediate. Assumes Kafka fundamentals.
**Platform:** Confluent Platform 7.9 (self-managed), KRaft mode — ZooKeeper-free.
**Hands-on:** 9 modules, **32 participant exercises** plus 5 instructor-led demonstrations,
one application built across all three days.

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

**Six of the nine modules cover capability that does not exist in Apache Kafka.** The
remaining three cover client development (producers, consumers, Connect) taught against
Confluent tooling, so that a mis-tuned producer is diagnosed in Control Center rather than
in a log file.

**This is a resumed delivery for the same participants** — the Intermediate course was
paused because its stack sat closer to Apache Kafka than to Confluent Platform, and this
outline is the rebuild on the platform you actually run.

## The course at a glance

| Day | Module | Focus | Exercises | Confluent-only subject |
|---|---|---|---|---|
| **1** | **1** | What Confluent Platform adds to Apache Kafka | 4 | Yes |
| **1** | **2** | Producers on Confluent Platform | 4 | Confluent tooling |
| **1** | **3** | Consumers, groups, and lag in Control Center | 4 | Confluent tooling |
| **2** | **4** | Confluent Schema Registry in depth | 4 | Yes |
| **2** | **5** | Data contracts and broker-side enforcement | 4 | Yes |
| **2** | **6** | Kafka Connect the Confluent way | 4 | Confluent tooling |
| **3** | **7** | ksqlDB | 4 | Yes |
| **3** | **8** | RBAC and secure development | 1 + 4 demos | Yes |
| **3** | **9** | Platform features that change application design | 3 + 1 demo | Yes |

Nine modules, **32 participant exercises**, plus a *Go further* stretch task in every
module. Six modules cover subject matter that does not exist in Apache Kafka; the other
three cover client development taught through Confluent tooling.

**Five items are instructor-led demonstrations rather than participant labs** — all of
Module 8 (RBAC) and exercise 9.2 (Cluster Linking). Both need infrastructure that belongs
to a platform team rather than a training VM: the Metadata Service with an enterprise
identity source, and a second cluster to link to. They are taught and shown, not
provisioned, which is also how developers meet them in practice.

## What is Confluent-specific, and where participants touch it

The table below is the course's answer to "is this just Apache Kafka?" — every row is a
capability the Apache distribution does not ship, mapped to the exercise where
participants use it themselves.

| Confluent Platform capability | In Apache Kafka | Exercises |
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
| **Tiered Storage** | KIP-405 exists in Apache 3.9+; Confluent's is older, with broader backend support | 9.1 |

> Stated plainly for the technical reviewer: **Tiered Storage and Connect are the two rows
> with a genuine Apache counterpart**, and they are labelled as such above. Every other row
> is capability that arrives only with Confluent Platform.

## How the course runs

This is a **workshop, not a lecture with exercises attached**. Every module follows the
same four-beat rhythm, and three of the four beats are hands-on:

| Beat | Time | What happens |
|---|---|---|
| **Probe** | ~15 min | A hands-on task participants cannot yet complete. They hit the wall first. |
| **Explain** | ~30 min | The concept — delivered as the answer to what they just ran into. |
| **Build** | ~50 min | The substantial lab. Working code or working configuration. |
| **Break it** | ~15 min | Deliberately break what they built and read the failure. |

**The teaching day is 08:30 to 16:30 with an hour for lunch.** Three modules a day, two
breaks, and time at the end of each afternoon to extend the running application. Timings
above are a rhythm, not a stopwatch — the instructor moves the boundary when a room needs
longer on something, which is the point of the *Go further* tasks.

The *Probe* is what makes this stick. Participants measure a slow producer before anyone
says the word `linger.ms`; they watch a consumer group leave two members idle before
anyone explains partition assignment. The explanation lands because they already have the
question.

**One application, three days.** Rather than nine disposable labs, participants build a
single event pipeline incrementally. Each module adds a stage to something that stays
running, so the last day is spent hardening a system they have owned since the first
morning rather than starting something new:

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
| **9** | It survives a broker loss with no data lost, replays history from tiered storage, and is reachable over HTTP |

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

# Day 1 — The Confluent Platform Developer Environment

*Getting productive inside the platform itself. Participants stop reaching for shell
scripts and start working the way they will at their desks — through Control Center and
the `confluent` CLI. The two client modules are the material that carries over from
general Kafka work, and they are taught here against Confluent tooling: a producer is
tuned by reading end-to-end latency in Control Center, and a broken consumer is diagnosed
from a lag chart rather than from a log file.*

**By the end of Day 1** participants have a running pipeline of their own — a tuned
producer writing events and a consumer group reading them with correct commit semantics —
and they can see all of it in Control Center.

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
- What transfers if the organisation later moves to Cloud

### 1.D Reading Confluent's documentation correctly

- Version mapping: CP 7.9 ≈ Kafka 3.9, CP 8.x ≈ Kafka 4.x
- Telling Platform docs from Cloud docs — the most common source of wasted time
- Spotting whether a documented feature is Community or Enterprise before relying on it

**Hands-on**

- **1.1 Probe** — Given a running cluster and nothing else: how many brokers, where do
  schemas live, who is consuming right now? Find out using only Control Center.
- **1.2 Build** — Create topics, inspect partitions and replicas, produce, and browse
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

**Hands-on**

- **2.1 Probe** — Run the supplied producer. Measure its throughput. Write your number on
  the whiteboard next to everyone else's. Nobody has said `linger.ms` yet.
- **2.2 Build** — Add the Confluent Monitoring Interceptor — two lines of config — and watch
  your producer appear in Control Center with end-to-end latency.
- **2.3 Build** — Sweep `linger.ms`, `batch.size`, and `compression.type`. Chart throughput
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
- `commitSync` and `commitAsync` — cost, blocking, and failure behaviour
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

**Hands-on**

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

# Day 2 — Data Contracts and Governance

*The day that most sharply separates Confluent Platform from Apache Kafka. The question
it answers is the one that actually costs enterprises money: how do you stop bad data
entering a topic that dozens of teams depend on? The answer builds in three steps — agree
the shape of the data, enforce that agreement at the broker where no client can bypass it,
then move data in and out of the platform without breaking the agreement.*

**By the end of Day 2** the pipeline carries registered, evolvable schemas, the broker
itself rejects anything off-contract, a PII field is encrypted, and data flows in from a
database and out to object storage with poison records routed aside rather than stopping
the pipeline.

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
- Migration realities when an organisation already has one of them

### 4.E Composition and portability

- Schema references — composing schemas instead of copying fields
- Schema Linking: moving schemas between development, test, and production
- Keeping registries consistent across environments

**Hands-on**

- **4.1 Probe** — Dump the raw bytes of a record. Find the magic byte and the schema ID.
  Resolve that ID against the registry by hand.
- **4.2 Build** — Register v1, run a consumer, evolve to v2 under BACKWARD compatibility,
  and confirm the old consumer survives untouched.
- **4.3 Break it** — Attempt an incompatible change and read the rejection. Switch the
  subject to FORWARD and watch which changes are now legal and which are not.
- **4.4 Build** — Manage schemas from Control Center and from the REST API; compare.
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
- What an unauthorised reader sees, including in the Control Center message browser
- Key management, and what happens to a consumer without the key
- Where CSFLE fits alongside TLS and disk encryption, which solve different problems

### 5.D Where enforcement belongs

- Client, broker, or both — the trade-offs stated plainly
- Performance and operational cost of broker-side validation
- Designing a rollout that does not break producers already in flight

**Hands-on**

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

**Hands-on**

- **6.1 Build** — Deploy a JDBC source connector from Postgres, configured and started
  entirely in Control Center.
- **6.2 Build** — Add an S3 sink. Verify the objects landed and inspect their format.
- **6.3 Break it** — Introduce a converter mismatch, the single most common Connect
  failure in production. Diagnose it from the failed task's trace in Control Center, then
  fix it.
- **6.4 Break it** — Send a poison record. Route it to a DLQ. Consume the DLQ and read the
  failure headers.
- *Go further* — Chain two SMTs to reshape records in flight, without touching the producer.

---

# Day 3 — Stream Processing, Security, and Design

*Turning a pipeline into an application. Processing moves into the stream rather than into
a nightly batch; the security model is made explicit; and the platform features that
genuinely change how a system is designed — effectively unlimited retention, mirrored
clusters, HTTP access — are examined for what they make possible rather than as
operational trivia. The day closes by breaking the whole thing on purpose.*

**By the end of Day 3** participants have enriched and aggregated their stream in ksqlDB
and queried it from Java, can write an access request their platform team could act on
without a follow-up conversation, and have watched their own application survive a broker
failure with no records lost.

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

**Hands-on**

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
- Consuming from a linked topic, and the failover contract an application must honour
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

**Hands-on**

- **9.1 Build** — Replay from a Tiered Storage offset older than local retention. Measure
  the cold-read latency penalty and decide whether you would accept it.
- **9.2 Demonstration** — Cluster Linking shown against a second cluster, which a single
  training VM cannot host. Consuming from a linked topic, and the failover contract an
  application must honour, walked through together.
- **9.3 Build** — Produce and consume over HTTP through the REST Proxy with nothing but
  `curl` — the integration path for non-JVM callers.
- **9.4 Capstone** — The full application under failure: schema-validated ingest, ksqlDB
  enrichment, Connect sink. Kill a broker mid-run and **prove zero loss from Control
  Center**. Then replay history from tiered storage.
- *Go further* — Add a quota, saturate it, and observe what your client does when throttled.

---

## Add-On Modules (optional Day 4, or substitutions)

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
| ksqlDB Server + CLI | `confluentinc/cp-ksqldb-server:7.9.9`, `cp-ksqldb-cli:7.9.9` | Day 3 |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.9.9` | JDBC + S3 from Confluent Hub |
| REST Proxy | `confluentinc/cp-kafka-rest:7.9.9` | Day 3 |
| Postgres, MinIO | `postgres:16`, `minio/minio` | Connect endpoints, not Kafka components |

**VM sizing — 4 vCPU, 16 GB RAM, 40 GB free disk per participant.** These are measured
numbers, not estimates: the Day 1 core alone (three brokers, Schema Registry, Control
Center) holds 4.5 GB resident, and Control Center accounts for the largest single share.
Days 2 and 3 add Connect, ksqlDB, and REST Proxy on top, alongside a Maven build and an
IDE. **A 12 GB VM is not sufficient** — Control Center starts on less and then fails
partway through the day under metrics load, which costs classroom time to diagnose.

Services are started by day rather than all at once, so no VM carries the full stack
before it is needed.

The participant VM deliberately does **not** host the Metadata Service, an identity
source, or a second cluster. Those are what Module 8 and exercise 9.2 demonstrate, and
they run on the instructor's environment rather than being provisioned sixteen times.
Keeping them off the student image is what holds the VM at 16 GB.

Client code is Java 17 + Maven, in a ready-made single Maven project — participants write
lab classes, not build files. A one-command startup script brings the environment up each
morning and verifies it is healthy before class begins.

All course materials — slides, labs, and runnable code — are supplied in a Git repository
that participants keep after the course.
