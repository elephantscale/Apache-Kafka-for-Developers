# Apache Kafka for Developers

**Format:** Introduction (1 day) + Intermediate (3 days) — deliverable together as a 4-day
track or as two standalone courses.
**Level:** Introductory through Intermediate. Hands-on, lab-intensive.
**Kafka version:** Apache Kafka 4.x (KRaft mode — ZooKeeper-free), current as of 2026.

---

## Overview

Apache Kafka is the de-facto standard for real-time data streaming — fault-tolerant,
horizontally scalable, and the backbone of modern event-driven architectures at LinkedIn,
Uber, Netflix, and thousands of enterprises. This track takes participants from Kafka
fundamentals through the intermediate developer skills needed to build production streaming
applications: producers and consumers, delivery guarantees and exactly-once semantics,
schema management, Kafka Connect pipelines, and stream processing.

The course is **current to Apache Kafka 4** — ZooKeeper has been fully removed (KRaft), and
the material reflects today's ecosystem (Flink SQL, Schema Registry, modern client APIs)
rather than legacy patterns.

The emphasis is **programming and hands-on practice** over cluster administration, with
every concept reinforced by a lab. All labs run in a self-contained environment; all
materials are provided in Git for participants to revisit and re-run afterward.

## Audience

Software developers and data engineers building or integrating with Kafka. Attendees should
be comfortable writing code (Java or Python examples provided) and working at the Linux
command line.

## Prerequisites

- Practical programming experience (Java and/or Python)
- Comfort with the Linux command line and a text editor
- Basic familiarity with messaging or data-processing concepts is helpful but not required
- *For the Intermediate course:* Kafka fundamentals (the Introduction course, or equivalent
  experience)

## Objectives

By the end of the track, participants will be able to:

- Explain Kafka's architecture and core abstractions (topics, partitions, offsets,
  brokers, consumer groups) and how KRaft replaces ZooKeeper
- Write producers and consumers, and reason about partitioning, ordering, and parallelism
- Choose and configure delivery guarantees — at-least-once, at-most-once, exactly-once
- Use idempotent and transactional producers for correctness
- Manage schemas and evolution with the Schema Registry (Avro, Protobuf, JSON Schema)
- Build data pipelines with Kafka Connect (source and sink connectors, error handling)
- Process streams with Kafka Streams and Flink SQL
- Apply reliability and performance basics: replication, ISR, consumer lag, tuning

---

# Introduction (1 Day)

*Kafka fundamentals for newcomers — concepts plus first hands-on.*

## Module 1 — What Is Kafka and Why

- Real-time streaming vs. batch; the problem Kafka solves
- Event-driven architecture: publish/subscribe vs. request/response
- Kafka use cases and who uses Kafka
- Where Kafka fits: the modern data platform

## Module 2 — Core Concepts

- Topics, partitions, and offsets — the log abstraction
- Producers, consumers, brokers
- Consumer groups and parallelism
- Replication, leaders, and fault tolerance (ISR) at a high level
- Retention: time, size, and log compaction

## Module 3 — Kafka 4 Architecture

- The Kafka cluster: brokers and the controller
- **KRaft — ZooKeeper-free Kafka** (what changed and why it matters)
- The Kafka ecosystem: Connect, Streams, Schema Registry, Flink

## Module 4 — Hands-On Fundamentals

- Start a Kafka cluster and inspect it
- Create topics with partitions and replication
- Produce and consume events from the command line
- Observe consumer-group partition assignment and lag

---

# Intermediate (3 Days)

*Developer-level skills for building production streaming applications.*

## Day 1 — Producers, Consumers & Delivery Guarantees

### Module 1 — Producer Internals
- Serialization; the partitioner (key-based vs. round-robin)
- Batching, `linger.ms`, `batch.size`, compression
- Acknowledgements: `acks=0/1/all` and durability trade-offs
- Idempotent producers — exactly-once at the producer

### Module 2 — Consumer Internals
- Consumer groups and rebalancing; the **KIP-848** next-gen protocol
- Offset management: auto vs. manual commit; `__consumer_offsets`
- Consumer positioning, seeking, and replay
- Handling rebalances and building resilient consumers

### Module 3 — Delivery Semantics & Transactions
- At-most-once, at-least-once, exactly-once — what each requires
- Transactions and the consume-process-produce loop
- `read_committed` vs. `read_uncommitted`
- **Hands-on:** build keyed producers/consumers; transactional pipeline

## Day 2 — Schemas & Data Integration

### Module 4 — Serialization & Schema Registry
- Message formats: JSON, Avro, Protobuf
- The Schema Registry: registering schemas, schema IDs, serdes
- Schema evolution and compatibility (BACKWARD / FORWARD / FULL)
- Data contracts as a governance practice
- **Hands-on:** produce/consume Avro with the Schema Registry; evolve a schema

### Module 5 — Kafka Connect
- Connect architecture: workers, connectors, tasks, internal topics
- Source and sink connectors; configuration
- Offset management and exactly-once source connectors
- Error handling, retries, and Dead Letter Queues
- Integration patterns: databases (JDBC/CDC), object storage (S3), search
- **Hands-on:** deploy source and sink connectors; trigger and inspect a DLQ

## Day 3 — Stream Processing & Reliability

### Module 6 — Stream Processing
- Kafka Streams: `KStream` / `KTable`, stateless vs. stateful operations
- Joins, aggregations, windowing, and state stores
- Exactly-once processing in Streams
- **Flink SQL** — declarative continuous queries (the modern successor to KSQL/ksqlDB)
- **Hands-on:** build a stateful streaming application

### Module 7 — Reliability, Scaling & Operations for Developers
- Replication factor, `min.insync.replicas`, and HA configuration
- Consumer lag as the primary health metric; monitoring basics
- Producer/consumer performance tuning
- Partition-count strategy and its effect on scaling and ordering
- **Hands-on / capstone:** end-to-end pipeline — ingest → process → sink — with
  reliability and monitoring

---

## Lab Environment

- Self-contained lab environment; Apache Kafka 4 (KRaft) with the supporting ecosystem
  (Schema Registry, Kafka Connect, Flink)
- All labs, code, and slides provided in a Git repository for participants to keep and
  re-run after the course

## Duration Options

- **4-day track:** Introduction (1 day) + Intermediate (3 days)
- **Standalone Introduction:** 1 day
- **Standalone Intermediate:** 3 days (assumes Kafka fundamentals)