# Confluent Platform for Developers

**Format:** 3 days, hands-on and lab-intensive. Optional 4th day (see *Add-On Modules*).
**Level:** Intermediate. Assumes Kafka fundamentals.
**Platform:** Confluent Platform 7.9 (self-managed), KRaft mode — ZooKeeper-free.

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

Roughly **half of the course covers capability that has no Apache Kafka equivalent**:
broker-side schema validation, Data Contracts, RBAC, Tiered Storage, Cluster Linking,
Self-Balancing Clusters, Confluent Monitoring Interceptors, and ksqlDB. The other half
covers client development — producers, consumers, delivery guarantees — taught against
Confluent tooling, so that a mis-tuned producer is diagnosed in Control Center rather
than in a log file.

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

The orientation module. Participants leave knowing exactly where the boundary is.

- The component map: `cp-server` vs. the Apache broker — what the enterprise broker adds
- Licensing tiers and why they matter to a developer: Apache 2.0, Confluent Community
  License, Confluent Enterprise License — which components fall where
- Confluent Platform vs. Confluent Cloud: the same APIs, different operational contract
- Version mapping: CP 7.9 ≈ Kafka 3.9, CP 8.x ≈ Kafka 4.x — reading Confluent's docs correctly
- The **`confluent` CLI**: contexts, `confluent kafka topic`, `confluent schema-registry`,
  `confluent iam`
- **Control Center** guided tour: cluster health, topics, message browser, consumer lag,
  schemas, connectors, ksqlDB editor

> **Lab 01 — Drive the platform, no shell scripts.** Bring up Confluent Platform. Create
> topics, inspect partitions and replicas, produce and browse messages, and inspect a
> consumer group — entirely from Control Center and the `confluent` CLI. Then do the same
> thing with `kafka-topics.sh` and discuss when each is appropriate.

## Module 2 — Producers on Confluent Platform

- Producer architecture: accumulator, batching, the sender thread
- The four configs that matter: `linger.ms`, `batch.size`, `compression.type`, `acks`
- Idempotence (default since Kafka 3.0), `max.in.flight.requests.per.connection`, ordering
- Partitioning and key hashing (murmur2); custom partitioners and when not to write one
- **Confluent Monitoring Interceptors** — two lines of client config that surface
  end-to-end latency and throughput in Control Center
- Reading a producer's behaviour from the outside: Control Center's throughput and
  latency panels vs. client-side JMX

> **Lab 02 — Tune a producer with the dashboard open.** Instrument a Java producer with the
> monitoring interceptor. Run it, watch Control Center. Change `linger.ms` and `batch.size`,
> re-run, and watch the latency/throughput trade-off move on screen. Break `acks` and watch
> durability change.

## Module 3 — Consumers, Groups, and Lag

- The poll loop; `max.poll.records`, `max.poll.interval.ms`, and the "stop-the-world" failure
- Commit strategies: auto, sync, async — and how commit *ordering* decides at-least-once
  vs. at-most-once
- `auto.offset.reset` and why it is ignored when a committed offset exists
- Rebalancing: eager vs. cooperative-sticky, and the KIP-848 protocol
- Rebalance listeners; offset seeking and replay
- **Consumer lag in Control Center** — reading it, alerting on it, and why LAG=0 does not
  mean your application processed the data
- **Confluent Parallel Consumer** — key-level concurrency beyond the partition count

> **Lab 03 — Lag that lies.** Build a consumer that commits before processing. Watch Control
> Center report LAG=0 while the application has handled a fraction of the records. Fix the
> commit ordering and watch the graph tell the truth. Then replay from a chosen offset.

---

# Day 2 — Data Contracts and Governance

*This is the day that most sharply separates Confluent Platform from Apache Kafka.*

## Module 4 — Confluent Schema Registry in Depth

- Subjects, versions, IDs, and the wire format — what those five bytes on the front of every
  record actually are
- Subject naming strategies: TopicName, RecordName, TopicRecordName — and the design
  consequences of each
- Compatibility modes: BACKWARD, FORWARD, FULL, TRANSITIVE — chosen from *who upgrades first*
- Avro, Protobuf, and JSON Schema serdes side by side; how to choose
- **Schema references** — composing schemas instead of copying them
- Managing schemas from Control Center; the Schema Registry REST API
- **Schema Linking** — moving schemas between environments

> **Lab 04 — Evolve a schema without breaking a consumer.** Register v1, run a consumer,
> evolve to v2 under BACKWARD compatibility, and confirm the old consumer survives. Then
> attempt an incompatible change and read the rejection. Repeat under FORWARD to see the
> opposite constraint.

## Module 5 — Data Contracts and Broker-Side Enforcement

The module that answers *"what stops a bad producer?"* — and the answer is Confluent-only.

- **Broker-side schema validation** (`confluent.value.schema.validation`) — a `cp-server`
  capability with no Apache Kafka equivalent
- What the producer sees when the broker rejects it, and how to handle that in code
- **Data Contracts**: schema metadata, domain rules, and validation rules
- **Client-Side Field-Level Encryption (CSFLE)** — encrypting a PII field inside the payload,
  with the key never reaching the broker
- Migration rules for transforming between schema versions in flight
- Where contract enforcement belongs: client, broker, or both

> **Lab 05 — The broker refuses your data.** Enable schema validation on a topic. Produce a
> well-formed record with a plain `StringSerializer` — bypassing Schema Registry entirely —
> and watch the *broker* reject it. Then encrypt a `ssn` field with CSFLE and confirm it is
> unreadable in the Control Center message browser while the authorized consumer still reads it.

## Module 6 — Kafka Connect the Confluent Way

- Workers, connectors, tasks; standalone vs. distributed; the internal topics
- **Confluent Hub**: community, commercially-licensed, and Confluent-supported connectors —
  and how to tell which you are installing
- Managing connectors from **Control Center**: deploy, pause, inspect, and read task failures
- Converters and their Schema Registry coupling — the single most common Connect failure
- Single Message Transforms; dead letter queues and error tolerance
- RBAC for Connect: what a connector's principal needs

> **Lab 06 — A pipeline you never leave the UI for.** Stand up a JDBC source from Postgres
> and an S3 sink, configured and monitored from Control Center. Break a record deliberately
> and route it to a DLQ. Inspect the failed task's stack trace in the UI.

---

# Day 3 — Stream Processing, Security, and Design

## Module 7 — ksqlDB

- Streams vs. tables — the duality, and why it is the whole idea
- `CREATE STREAM` / `CREATE TABLE`, and the topic-backed reality underneath
- **Push queries** (`EMIT CHANGES`) vs. **pull queries** — continuous vs. point-in-time
- Joins: stream-stream (windowed), stream-table (enrichment), table-table
- Aggregation and windowing: tumbling, hopping, session
- Materialized views and querying them from an application over REST
- Exactly-once in ksqlDB; scaling by adding servers
- The ksqlDB editor and query lineage in Control Center
- **Where Confluent is heading:** Apache Flink is Confluent's strategic stream-processing
  engine. When to reach for ksqlDB, Kafka Streams, or Flink

> **Lab 07 — Rebuild yesterday's Java pipeline in SQL.** Take the enrichment pipeline written
> in Java on Day 1 and express it in ksqlDB in a fraction of the code. Run a push query, then
> a pull query against the materialized view. Compare the two approaches honestly.

## Module 8 — RBAC and Secure Development

- The Metadata Service (MDS) and how Confluent Platform authorization actually works
- Principals, service accounts, and role bindings
- The developer-relevant roles: `DeveloperRead`, `DeveloperWrite`, `DeveloperManage`,
  `ResourceOwner` — scoped to topics, consumer groups, subjects, and connectors
- Reading an authorization failure: what `TopicAuthorizationException` means and who to ask
- What a developer requests from the platform team, and how to ask for it precisely
- Secrets in client configs; SASL mechanisms in a CP deployment

> **Lab 08 — Denied, then allowed.** Run a client as an unprivileged principal and read the
> exact authorization failure. Create the role binding that fixes it. Re-run. Then narrow the
> binding until it breaks again, to see the boundary.

## Module 9 — Platform Features That Change Application Design

Operational features, taught from the developer's side: each one changes what you are
allowed to assume.

- **Tiered Storage** — when retention is effectively infinite, replay-from-origin becomes a
  design pattern rather than an emergency. What it costs in latency on a cold read
- **Cluster Linking** — consuming a topic that lives on another cluster; active-passive DR
  and the failover contract. Compared honestly with MirrorMaker 2
- **Self-Balancing Clusters** — why developers no longer hand-write partition reassignment JSON
- **REST Proxy** — producing and consuming over HTTP for callers that cannot host a Java client
- Health+ and the metrics your operations team watches
- Quotas: what happens to your client when it is throttled

> **Lab 09 — Capstone.** An end-to-end pipeline under failure: schema-validated ingest,
> ksqlDB enrichment, Connect sink, RBAC-scoped principals. Kill a broker mid-run and prove
> zero loss from Control Center. Replay from a tiered-storage offset older than local retention.

---

## Add-On Modules (optional Day 4, or substitutions)

| Module | Fits when |
|---|---|
| **Kafka Streams in Java** | Teams building stateful services in the JVM rather than SQL |
| **Flink SQL on Confluent** | Teams tracking Confluent's strategic direction beyond ksqlDB |
| **Confluent Cloud** | Teams evaluating a managed path — Stream Governance, Catalog, Lineage |
| **Migrating from OSS Kafka to CP** | If teams are still running Apache clusters alongside |
| **Confluent for Kubernetes (CFK)** | Sites deploying Confluent Platform via the operator |
| **Performance tuning workshop** | High-volume fan-out, partition sizing, consumer scaling |

---

## Lab Environment

A single Docker Compose stack per participant, on a provided VM.

| Service | Image | Notes |
|---|---|---|
| Brokers ×3 | `confluentinc/cp-server:7.9.8` | KRaft mode, combined broker+controller |
| Control Center | `confluentinc/cp-enterprise-control-center:7.9.8` | Primary UI for the whole course |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.8` | |
| ksqlDB Server + CLI | `confluentinc/cp-ksqldb-server:7.9.8`, `cp-ksqldb-cli:7.9.8` | Day 3 |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.9.8` | JDBC + S3 from Confluent Hub |
| REST Proxy | `confluentinc/cp-kafka-rest:7.9.8` | Day 3 |
| Postgres, MinIO | `postgres:16`, `minio/minio` | Connect endpoints, not Kafka components |

Client code is Java 17 + Maven, in a ready-made single Maven project — participants
write lab classes, not build files. All course materials — slides, labs, and runnable
code — are supplied in a Git repository that participants keep after the course.
