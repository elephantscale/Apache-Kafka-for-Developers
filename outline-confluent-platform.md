# Confluent Platform for Developers

**Format:** 3 days. **Approximately two-thirds of class time is spent at the keyboard.**
Optional 4th day (see *Add-On Modules*).
**Level:** Intermediate. Assumes Kafka fundamentals.
**Platform:** Confluent Platform 7.9 (self-managed), KRaft mode — ZooKeeper-free.
**Hands-on:** 9 modules, **36 exercises**, one application built across all three days.

---

## Description

Apache Kafka is the protocol. **Confluent Platform is the product enterprises actually
run** — and the distance between the two is where a great deal of developer time is spent.
This course closes it.

Every module is delivered on a real Confluent Platform cluster: `cp-server` brokers,
Control Center, Confluent Schema Registry, ksqlDB, Kafka Connect with Confluent Hub,
REST Proxy, and RBAC. Participants drive the platform through **Control Center and the
`confluent` CLI** — the same interfaces they use at their desks. The Apache CLI tools
(`kafka-topics.sh` and friends) appear only where they are still the right tool, and are
named as such.

**Six of the nine modules — 24 of the 36 exercises — cover capability that does not exist
in Apache Kafka.** The remaining three cover client development (producers, consumers,
Connect) taught against Confluent tooling, so that a mis-tuned producer is diagnosed in
Control Center rather than in a log file.

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
| **RBAC via the Metadata Service** — roles scoped to topics, groups, subjects, connectors | ACLs only — no roles, no principals service | 8.1 – 8.4 |
| **Cluster Linking** — byte-for-byte topic mirroring, offset-preserving | MirrorMaker 2, with offset translation caveats | 9.2 |
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
| **Probe** | ~10 min | A hands-on task participants cannot yet complete. They hit the wall first. |
| **Explain** | ~20 min | The concept — delivered as the answer to what they just ran into. |
| **Build** | ~40 min | The substantial lab. Working code or working configuration. |
| **Break it** | ~15 min | Deliberately break what they built and read the failure. |

The *Probe* is what makes this stick. Participants measure a slow producer before anyone
says the word `linger.ms`; they watch a consumer group leave two members idle before
anyone explains partition assignment. The explanation lands because they already have the
question.

**One application, three days.** Rather than nine disposable labs, participants build a
single event pipeline incrementally: ingest → schema-validated contract → enrichment →
sink → secured → survivable. Each module adds a stage to something that stays running.
By Friday afternoon they have an application, not a folder of snippets.

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

## Objectives

By the end of the course, participants will be able to:

- Name every component of Confluent Platform, what it costs, and when to reach for it
- Operate the platform as a developer through **Control Center** and the **`confluent` CLI**
- Write, tune, and diagnose producers and consumers using **Confluent Monitoring Interceptors**
  and Control Center's end-to-end latency and consumer-lag views
- Choose and configure delivery guarantees, including exactly-once with transactions
- Design and evolve **data contracts** in Confluent Schema Registry — compatibility modes,
  schema references, rules, and field-level encryption
- Enforce those contracts **at the broker** with schema validation, so bad data cannot land
- Build pipelines with Kafka Connect and Confluent Hub connectors, managed from Control Center
- Write stream processing in **ksqlDB** — streams, tables, push and pull queries, materialized views
- Work within **RBAC**: understand principals, role bindings, and what to request from the
  platform team
- Exploit **Tiered Storage** and **Cluster Linking** as application design options, not just
  operational features

---

# Day 1 — The Confluent Platform Developer Environment

## Module 1 — What Confluent Platform Adds to Apache Kafka

**Concept.** The component map: `cp-server` vs. the Apache broker. Licensing tiers and why
a developer cares — Apache 2.0, Confluent Community, Confluent Enterprise. Confluent
Platform vs. Confluent Cloud. Version mapping (CP 7.9 ≈ Kafka 3.9, CP 8.x ≈ Kafka 4.x) and
how to read Confluent's docs correctly.

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

**Concept.** Producer architecture: accumulator, batching, sender thread. The four configs
that matter — `linger.ms`, `batch.size`, `compression.type`, `acks`. Idempotence (default
since Kafka 3.0), `max.in.flight.requests.per.connection`, ordering. Partitioning and
murmur2 key hashing. Confluent Monitoring Interceptors.

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

**Concept.** The poll loop; `max.poll.records`, `max.poll.interval.ms`, and the
stop-the-world failure. Commit strategies and how commit *ordering* decides at-least-once
vs. at-most-once. `auto.offset.reset` and why it is ignored when a committed offset exists.
Rebalancing: eager, cooperative-sticky, KIP-848. Consumer lag in Control Center.

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

*The day that most sharply separates Confluent Platform from Apache Kafka.*

## Module 4 — Confluent Schema Registry in Depth

**Concept.** Subjects, versions, IDs, and the wire format. Subject naming strategies —
TopicName, RecordName, TopicRecordName — and their design consequences. Compatibility
modes chosen from *who upgrades first*. Avro, Protobuf, and JSON Schema side by side.
Schema references. Schema Linking across environments.

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

**Concept.** Broker-side schema validation — a `cp-server` capability with no Apache Kafka
equivalent. What the producer sees when the broker rejects it. Data Contracts: schema
metadata, domain rules, validation rules. Client-Side Field-Level Encryption. Migration
rules. Where enforcement belongs — client, broker, or both.

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

**Concept.** Workers, connectors, tasks; distributed mode and the internal topics.
Confluent Hub: community, commercially-licensed, and Confluent-supported connectors, and
how to tell which you are installing. Converters and their Schema Registry coupling.
Single Message Transforms. Dead letter queues. RBAC for Connect principals.

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

## Module 7 — ksqlDB

**Concept.** Streams vs. tables and why the duality is the whole idea. Push queries
(`EMIT CHANGES`) vs. pull queries. Joins: stream-stream, stream-table, table-table.
Windowing: tumbling, hopping, session. Materialized views. Exactly-once in ksqlDB.
Where Confluent is heading with Apache Flink, and when to choose ksqlDB, Kafka Streams,
or Flink.

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

**Concept.** The Metadata Service and how Confluent Platform authorization actually works.
Principals, service accounts, role bindings. The developer-relevant roles — `DeveloperRead`,
`DeveloperWrite`, `DeveloperManage`, `ResourceOwner` — scoped to topics, groups, subjects,
and connectors. Reading an authorization failure. Secrets in client configs.

**Hands-on**

- **8.1 Probe** — Run your working application as an unprivileged principal. Read the exact
  exception. Work out precisely which permission is missing.
- **8.2 Build** — Create the role binding that fixes it. Re-run. It works.
- **8.3 Break it** — Narrow the binding one scope at a time until it breaks again. You have
  now found the true minimum privilege, empirically.
- **8.4 Build** — Scope the Connect connector's own principal from Module 6.
- *Go further* — Write the access request you would actually send your platform team,
  with exact resource names and roles.

## Module 9 — Platform Features That Change Application Design

**Concept.** Tiered Storage and what effectively-infinite retention does to your design
options. Cluster Linking versus MirrorMaker 2. Self-Balancing Clusters. REST Proxy for
callers that cannot host a Java client. Health+ and what your operations team watches.
Quotas and what throttling does to your client.

**Hands-on**

- **9.1 Build** — Replay from a Tiered Storage offset older than local retention. Measure
  the cold-read latency penalty and decide whether you would accept it.
- **9.2 Build** — Consume a topic that lives on another cluster via Cluster Linking.
  Walk the failover contract.
- **9.3 Build** — Produce and consume over HTTP through the REST Proxy with nothing but
  `curl` — the integration path for non-JVM callers.
- **9.4 Capstone** — The full application under failure: schema-validated ingest, ksqlDB
  enrichment, Connect sink, RBAC-scoped principals. Kill a broker mid-run and **prove zero
  loss from Control Center**. Then replay history from tiered storage.
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

Client code is Java 17 + Maven, in a ready-made single Maven project — participants write
lab classes, not build files. A one-command startup script brings the environment up each
morning and verifies it is healthy before class begins.

All course materials — slides, labs, and runnable code — are supplied in a Git repository
that participants keep after the course.
