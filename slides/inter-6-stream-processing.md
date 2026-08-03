# Intermediate 6 — Stream Processing

Elephant Scale

---

## Agenda

- Kafka Streams: `KStream` / `KTable`, stateless vs. stateful operations
- Joins, aggregations, windowing, and state stores
- Exactly-once processing in Streams
- Flink SQL — declarative continuous queries (the modern successor to ksqlDB)
- Hands-on: build a stateful streaming application

---

## Beyond Produce/Consume: Processing In Flight

So far: producers write, consumers read, Connect moves data. **Stream processing** transforms
the data **as it flows** — filter, enrich, aggregate, join — continuously.

```
   raw events ──► [ transform / aggregate / join ] ──► derived events
                        continuously, in real time
```

- A consumer that just filters is fine — but joins, windowed aggregations, and stateful counts
  are hard to hand-roll correctly (state, fault tolerance, exactly-once)
- Stream processors give you those as **operators**, with state and recovery handled for you
- This is exactly what real-time **reporting and analytics** need — compute metrics as events
  arrive, not in a nightly batch

Notes:
This module is where SSA's "real-time reporting/analytics" and "did that person call?"
correlation actually get built. Frame it as the payoff of the whole course.

---

## Two Tools, One Job

We'll look at two ways to process Kafka streams:

- **Kafka Streams** — a **Java library** you embed in your application. No separate cluster; your
  app *is* the processor. Great when the logic lives inside a Java service.
- **Flink SQL** — a powerful stream processor where you write **SQL**. Declarative, language-
  neutral, runs on a Flink cluster. The modern successor to KSQL/ksqlDB.

| | Kafka Streams | Flink SQL |
|---|---|---|
| Form | Java library | SQL on a Flink cluster |
| Deploy | inside your app | submit to Flink |
| Best for | logic embedded in a Java service | analysts/engineers writing declarative queries |

Both read Kafka, process continuously, and write results back to Kafka.

---

## Kafka Streams: KStream

A **`KStream`** models an unbounded **stream of events** — every record is an independent fact.

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> orders = builder.stream("orders");

orders
  .filter((key, value) -> value.contains("CONFIRMED"))   // stateless
  .mapValues(value -> value.toUpperCase())               // stateless
  .to("orders-confirmed");
```

- Each record flows through the topology independently
- **Stateless** operations — `filter`, `map`, `mapValues`, `flatMap`, `branch` — need no memory
  of past records
- This is the easy half: record in, record out

---

## KStream vs. KTable

The key distinction in Kafka Streams:

- **`KStream`** = a stream of **events** (append) — "order 5 was placed", "order 5 was placed
  again" are two facts
- **`KTable`** = a **changelog** of **state** (upsert by key) — "the latest status of order 5" —
  a new record for a key **replaces** the old

```
KStream "clicks"      : (user1, click) (user1, click) (user2, click)   ← every event
KTable  "user-status" : (user1, ACTIVE) (user1, AWAY)                   ← latest per key
                         user1's value is now AWAY (ACTIVE replaced)
```

- Use a **KStream** for events/facts; a **KTable** for current state / lookups
- A compacted topic + a KTable = "materialized current state," rebuildable from the log

Notes:
Tie back to Intro 2 log compaction — a KTable is the programmatic face of a compacted
"latest-per-key" topic.

---

## Stateful Operations: Aggregations

Aggregations **remember** — counts, sums, running metrics per key. That memory is **state**.

```java
KStream<String, String> claims = builder.stream("claims");

claims
  .groupBy((key, value) -> regionOf(value))     // key by region
  .count()                                       // stateful: count per region
  .toStream()
  .to("claims-per-region");
```

- `count`, `reduce`, `aggregate` maintain a running result **per key**
- The result is a **KTable** (the current aggregate), backed by a **state store**
- "Claims per region," "orders per customer," "no-show rate per office" — all aggregations

---

## State Stores and Fault Tolerance

Where does that state live, and what happens if the app crashes?

- Aggregations keep state in a local **state store** (RocksDB on disk, by default)
- Every update is also written to a **changelog topic** in Kafka
- On restart or reassignment, the state store is **rebuilt from the changelog** — no lost counts

```
   aggregate ──► local state store (fast) ──► changelog topic (durable)
   crash ──► new instance ──► restore state store from changelog ──► resume
```

**Takeaway:** the stream processor gives you durable, recoverable state — the hard part you'd
otherwise hand-build.

---

## Windowing

Aggregating "since the beginning of time" is rarely what you want. **Windows** bucket events by
time — the basis of real-time metrics.

- **Tumbling** — fixed, non-overlapping (e.g. count per 1-minute bucket)
- **Hopping** — fixed size, overlapping (every 30s, count the last 1 min)
- **Session** — grouped by activity gaps (a user's burst of clicks)

```
tumbling, 1-min:   |--10:00--|--10:01--|--10:02--|
                    events → counted in their bucket
```

- "Claims per hour," "appointments per office per 15 minutes" — windowed aggregations
- Windows need a notion of **event time** and handling for **late** events (watermarks)

Notes:
Windowing is the heart of real-time reporting. SSA's "claims per hour/region" is a
tumbling-window count.

---

## Joins

**Joins** correlate two streams/tables by key — combining related events.

- **Stream–Table** — enrich each event with current state (event ⋈ lookup table)
- **Stream–Stream** — correlate two event streams **within a time window**
- **Table–Table** — keep a joined materialized view up to date

```
   claims stream   ⋈   contacts stream   (by person id, within 1 hour)
   → "claim submitted AND this person called within the hour"
```

- Stream–stream joins are **windowed** (you can't wait forever for a match)
- This is exactly the "did that person call?" correlation — join the claim and contact streams
  by person id

Notes:
This slide is the SSA correlation use case. A bare consumer can't do this cheaply; a
stream processor's windowed join is built for it.

---

## Exactly-Once in Kafka Streams

Everything from Module 7 (transactions) is available in Streams — with **one config line**.

```java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

- Streams uses transactions under the hood: input offsets, state-store changelog, and output
  records all commit **atomically**
- A crash mid-processing leaves **no** partial output and **no** double-counting
- You get exactly-once **stateful** processing without writing the transaction code yourself

**This is the big win:** the hardest correctness problem in streaming, reduced to a setting.

---

## Flink SQL: Streams as Tables

Flink treats a Kafka topic as a **table** you query with SQL — but the query runs **forever**,
emitting results as new data arrives.

```sql
-- a Kafka topic, as a table
CREATE TABLE claims (
  claim_id INT, person_id STRING, region STRING, amount DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'claims',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

-- a CONTINUOUS query — never "finishes"
SELECT region, COUNT(*) FROM claims GROUP BY region;
```

- No Java to compile — analysts and engineers write **SQL**
- The same concepts (filter, aggregate, window, join) as declarative queries

---

## Flink SQL: Windows and Sinks

Real-time analytics in a few lines of SQL.

```sql
-- tumbling 1-minute count per region (windowed aggregation)
SELECT window_start, region, COUNT(*) AS n
FROM TABLE(TUMBLE(TABLE claims, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, region;

-- continuous ETL: write results back to a Kafka topic
INSERT INTO claims_per_region
SELECT region, COUNT(*) FROM claims GROUP BY region;
```

- `INSERT INTO` a Kafka-backed table = a running pipeline that keeps the sink topic current
- This is how a dashboard stays live — Flink writes the aggregate topic, the dashboard reads it

Notes:
This is Lab 07's spine — source table, filter, windowed count, and INSERT INTO a sink,
all in SQL. It maps 1:1 to their real-time reporting goal.

---

## Choosing: Streams or Flink SQL

Both are right in different places.

- **Kafka Streams** when the processing logic belongs **inside a Java service** you already run —
  no extra cluster, tight integration with app code
- **Flink SQL** when you want **declarative** analytics, a shared query language for a team, or a
  dedicated processing platform separate from the apps
- They coexist: services embed Streams for operational logic; Flink SQL powers analytics/reporting

**For real-time reporting and analytics, Flink SQL is often the faster path** — which is why the
lab uses it.

---

## Summary

- Stream processing transforms data **in flight** — filter, enrich, aggregate, join — continuously
- **Kafka Streams** (Java library): `KStream` (events) vs `KTable` (state); stateless ops vs
  **stateful** aggregations backed by fault-tolerant **state stores**
- **Windows** (tumbling/hopping/session) make real-time metrics; **joins** correlate streams
  (the "did they call?" case)
- **Exactly-once** stateful processing is one config line in Streams (`EXACTLY_ONCE_V2`)
- **Flink SQL** expresses the same ideas as **continuous SQL** — source tables, windows, and
  `INSERT INTO` sinks — the modern successor to ksqlDB and a fast path to real-time analytics
