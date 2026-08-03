# Intermediate 5 — Kafka Connect

Elephant Scale

---

## Agenda

- Connect architecture: workers, connectors, tasks, internal topics
- Source and sink connectors; configuration
- Offset management and exactly-once source connectors
- Error handling, retries, and Dead Letter Queues
- Integration patterns: databases (JDBC/CDC), object storage (S3), search
- Hands-on: deploy source and sink connectors; trigger and inspect a DLQ

---

## The Integration Problem

Most Kafka data doesn't originate in Kafka — it lives in databases, and it needs to land in
warehouses, search indexes, and object stores. Writing that plumbing by hand, over and over,
is where projects bleed time.

```
   Postgres ──?──► Kafka ──?──► S3
   MySQL    ──?──► Kafka ──?──► Elasticsearch
   ...each arrow a bespoke producer/consumer app to build, deploy, and operate
```

- Every source and sink becomes a custom app: retries, offsets, scaling, monitoring — all DIY
- The same patterns get reimplemented on every team
- **Kafka Connect** turns that plumbing into **configuration**

Notes:
For SSA's "several systems sharing events," Connect is how you get data OUT of those
systems into Kafka without asking each team to write and run a bespoke producer.

---

## What Kafka Connect Is

A **framework and runtime** for streaming data between Kafka and external systems using
ready-made, reusable **connectors** — no client code.

- **Source** connectors pull data **into** Kafka (e.g. a database table → a topic)
- **Sink** connectors push data **out of** Kafka (e.g. a topic → S3)
- You submit **JSON configuration** to a REST API; Connect runs it
- It handles scaling, offset tracking, retries, restarts, and error routing for you

```
   sources ──[ source connectors ]──► Kafka ──[ sink connectors ]──► sinks
                     Kafka Connect cluster (workers)
```

---

## Architecture: Workers, Connectors, Tasks

Four terms, one hierarchy:

- **Worker** — a Connect JVM process; a Connect **cluster** is one or more workers
- **Connector** — a configured integration (e.g. "the orders JDBC source"); it decides how to
  split the work
- **Task** — the unit that actually moves data; a connector runs **N tasks** in parallel
- Tasks are **distributed** across workers and **rebalanced** if a worker fails

```
   Connect cluster
   ┌───────── worker 1 ─────────┐   ┌───────── worker 2 ─────────┐
   │  orders-source: task 0     │   │  orders-source: task 1     │
   │  s3-sink: task 0           │   │  s3-sink: task 1           │
   └────────────────────────────┘   └────────────────────────────┘
```

Notes:
This is the same "scale by parallel tasks, rebalance on failure" model as consumer
groups — sink tasks literally ARE a consumer group under the hood.

---

## Distributed Mode and Internal Topics

Production Connect runs in **distributed mode**, and — very Kafka — it stores its own state
**in Kafka topics**.

- `connect-configs` — connector configurations
- `connect-offsets` — source connector progress (where each source left off)
- `connect-status` — connector/task status

Because state lives in Kafka:

- Workers are **stateless** — one can die and its tasks move to another with no data loss
- You submit config once via REST; the cluster remembers it
- Sink connectors track their position with normal **consumer offsets** (`__consumer_offsets`)

---

## Configuring a Connector

A connector is just JSON, POSTed to the Connect REST API (`:8083`).

```json
{
  "name": "orders-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/orders_db",
    "table.whitelist": "orders",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "updated_at",
    "incrementing.column.name": "id",
    "topic.prefix": "pg-"
  }
}
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @orders-source.json http://localhost:8083/connectors
```

- `connector.class` selects the plugin; the rest is that plugin's configuration
- Manage the lifecycle over REST: list, status, pause, resume, restart, delete

---

## Source Connectors: Getting Data In

A source connector reads an external system and produces to Kafka.

- The **JDBC source** polls a table with `SELECT … WHERE` on a tracking column
  - `incrementing` — an auto-increment id (new rows only)
  - `timestamp` — an `updated_at` column (new **and** updated rows)
  - `timestamp+incrementing` — both, the robust default
- It records how far it read in `connect-offsets`, so a restart resumes cleanly

**Important limit:** a JDBC *poll* source sees only committed rows via query — it **cannot see
DELETEs**, and only catches updates if a timestamp column changes. For true change capture you
need **CDC**.

---

## CDC vs. Poll-Based Sources

Two ways to get database changes into Kafka:

| | JDBC poll source | CDC (e.g. Debezium) |
|---|---|---|
| How | periodic `SELECT` on a tracking column | tails the DB **transaction log** (WAL) |
| Sees | inserts, timestamped updates | inserts, updates, **and deletes** |
| Load | query load on the DB | low, reads the log |
| Setup | simple | more moving parts |

- **Poll** is fine for append-mostly tables and this course's labs
- **CDC** is the production answer when you need every change, including deletes, with low DB
  impact

Notes:
Name-drop Debezium as the standard CDC option. The lab uses JDBC poll because it's
self-contained, but flag the DELETE blind spot explicitly.

---

## Sink Connectors: Getting Data Out

A sink connector is a **consumer group** that writes each record to an external system.

- The **S3 sink** batches topic records into files in object storage (our lab uses MinIO, an
  S3-compatible store)
- Others: Elasticsearch/OpenSearch (search), JDBC sink (into a database), and many more
- Sink tasks commit **consumer offsets**, so they resume where they left off
- Scaling = more tasks (up to the topic's partition count — the familiar ceiling)

```
   topic "pg-orders" ──► [ S3 sink: task 0, task 1 ] ──► s3://kafka-data-lake/…
```

---

## Exactly-Once in Connect

Connect can provide strong delivery guarantees — with the same trade-offs you already know.

- **Source** connectors support **exactly-once** (Kafka 3.3+): they use transactions to atomically
  produce records and commit source offsets — no duplicate or lost source records
- **Sink** connectors are typically **at-least-once**; exactly-once to the destination depends on
  the sink being **idempotent** (upserts, dedup keys) — the same boundary as Module 7
- Delivery to an external system is only as exactly-once as that system allows

**Takeaway:** exactly-once *into* Kafka is built in; exactly-once *out* depends on the sink.

---

## Error Handling and Dead Letter Queues

Real pipelines hit bad records — malformed JSON, a schema mismatch, a value the sink rejects.
Connect's error handling decides what happens.

- Default: a bad record **fails the task** (safe, but one poison record stops the pipeline)
- **Tolerant mode** + a **Dead Letter Queue (DLQ)**: route bad records to a separate topic and
  keep going

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq-orders",
"errors.deadletterqueue.context.headers.enable": "true"
```

- Good records flow; bad ones land in `dlq-orders` with headers explaining **why**
- You monitor and reprocess the DLQ out of band — the pipeline never stalls on one bad message

Notes:
The DLQ is the single most important operational feature here. Lab 06 injects a bad
record and inspects it in the DLQ with its error headers.

---

## Single Message Transforms (SMTs)

Lightweight, per-record tweaks applied **inside** Connect — no separate stream processor.

- Rename/insert/mask fields, route to topics, cast types, add timestamps
- Configured inline on the connector; run per record as it passes through

```json
"transforms": "addPrefix",
"transforms.addPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.addPrefix.regex": "(.*)",
"transforms.addPrefix.replacement": "ingest-$1"
```

- Great for small shaping (mask a SSN, drop a column, route by name)
- For real joins/aggregations, use **stream processing** (next module) — SMTs are per-record only

---

## Integration Patterns

Where Connect fits in a real architecture:

- **Databases** — JDBC source/sink, or **CDC** (Debezium) for full change capture
- **Object storage** — S3/GCS/Azure sinks for a data lake or archival (our lab: MinIO)
- **Search** — Elasticsearch/OpenSearch sinks for real-time indexing
- **Warehouses** — sinks into Snowflake/BigQuery/Redshift for analytics
- **When NOT to use Connect** — if you need rich per-record business logic, a real app or a
  stream processor may fit better than a connector + SMTs

Notes:
Tie to SSA: several source systems → Connect sources → Kafka → sink to a store that powers
their real-time reporting. Connect is the integration spine.

---

## Summary

- **Kafka Connect** streams data in/out of Kafka as **configuration**, not custom apps
- **Workers** run **connectors**, which split work into parallel **tasks**; state lives in Kafka
  topics, so workers are stateless and fault-tolerant
- **Source** connectors pull in (JDBC poll vs. **CDC** for deletes); **sink** connectors push out
  (a consumer group under the hood)
- **Exactly-once** is built into sources; sink exactly-once needs an **idempotent** destination
- **DLQs** keep pipelines running past bad records; **SMTs** do lightweight per-record shaping
- Connect is the **integration spine** for databases, object storage, search, and warehouses
