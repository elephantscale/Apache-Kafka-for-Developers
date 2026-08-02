# Lab 7 — Stream Processing with Flink SQL

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 6 — Stream Processing
- **Duration:** ~75 minutes
- **Difficulty:** Intermediate / Advanced
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Tools:** Apache Flink SQL client; a small Java producer to feed events

## Objectives

By the end of this lab you will be able to:

- Model a Kafka topic as a Flink **table** and run continuous queries over it
- Run a stateless filter and a stateful aggregation (count per key)
- Compute a **windowed** aggregation (tumbling window) — the basis of real-time metrics
- Write results back to Kafka with `INSERT INTO` (a live pipeline)
- Correlate two streams with a **windowed join** ("was there a matching event?")

## Prerequisites

- The core cluster running, plus the **flink profile**:
  ```bash
  docker compose up -d
  docker compose --profile flink up -d
  docker compose ps          # flink-jobmanager, flink-taskmanager present
  ```
- Flink Web UI reachable at <http://localhost:8082>
- JDK 17 + Maven (for the Java event feeder) — see [`labs/SETUP.md`](../SETUP.md)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster with the **flink** profile: a Flink
> JobManager + TaskManager with the `flink-sql-connector-kafka` jar on the classpath, so Flink
> SQL can read and write Kafka topics. You run SQL in the **Flink SQL client** inside the
> JobManager container; a small **Java** producer on the host feeds JSON events into Kafka. Flink
> reaches Kafka as `kafka-1:9092` (inside the docker network); your host producer uses
> `localhost:9092`.

### Create the lab topics

```bash
for t in lab07-claims lab07-contacts lab07-claims-per-region; do
  docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $t --partitions 3 --replication-factor 3
done
```

### Open the Flink SQL client

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

You'll get a `Flink SQL>` prompt. Keep this open; you'll run all SQL here. (Type `SET
'sql-client.execution.result-mode' = 'tableau';` for readable output.)

---

## Exercise 1 — A Kafka Topic as a Flink Table

> **What this shows:** Flink models a topic as a table. Once declared, you query it with SQL —
> but the query runs continuously, emitting rows as events arrive. This declares the `claims`
> source.

### 1.1 Declare the source table (in the SQL client)

```sql
CREATE TABLE claims (
  claim_id   INT,
  person_id  STRING,
  region     STRING,
  amount     DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'lab07-claims',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'properties.group.id' = 'flink-claims',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);
```

### 1.2 Feed events (Java producer, in another terminal)

Add this to your Maven project (same `pom.xml` as Lab 05) and run it — it produces JSON claims
with an `event_time`:

```java
// src/main/java/com/elephantscale/kafka/ClaimFeeder.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class ClaimFeeder {
  public static void main(String[] args) throws Exception {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    String[] regions = {"EAST", "WEST", "SOUTH"};
    Random r = new Random();

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 1; i <= 200; i++) {
        String region = regions[r.nextInt(regions.length)];
        String person = "person-" + r.nextInt(20);
        String ts = Instant.now().toString().replace("Z", "").substring(0, 23).replace("T", " ");
        String json = String.format(
            "{\"claim_id\":%d,\"person_id\":\"%s\",\"region\":\"%s\",\"amount\":%.2f,\"event_time\":\"%s\"}",
            i, person, region, 10 + r.nextInt(490) + r.nextDouble(), ts);
        producer.send(new ProducerRecord<>("lab07-claims", person, json));
        Thread.sleep(200);   // ~5 events/sec
      }
    }
    System.out.println("fed 200 claims");
  }
}
```

```bash
cd lab05    # or wherever your Maven project is
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ClaimFeeder
```

### 1.3 Query the live stream

Back in the SQL client:

```sql
SELECT * FROM claims;
```

Rows stream in as the feeder produces. Press `Q` to stop the query (the feeder keeps running).

> **If a sharp student asks:** what is the `WATERMARK` line for? It tells Flink how to reason
> about **event time** and lateness — "assume no event is more than 5s late." Watermarks are what
> let windowed aggregations know when a time bucket is complete enough to emit.

---

## Exercise 2 — Stateless Filter and Stateful Count

> **What this shows:** the two halves of processing. A filter is stateless (row in → row out). A
> `GROUP BY` count is stateful — Flink maintains a running count per key and updates it as events
> arrive.

### 2.1 Stateless filter

```sql
SELECT claim_id, region, amount
FROM claims
WHERE amount > 250;
```

Only high-value claims appear. Stop with `Q`.

### 2.2 Stateful aggregation (running count per region)

```sql
SELECT region, COUNT(*) AS claim_count, ROUND(AVG(amount), 2) AS avg_amount
FROM claims
GROUP BY region;
```

Watch the counts **update live** as the feeder runs — this is a continuously maintained
aggregate, not a one-shot query.

> **If a sharp student asks:** why do the numbers keep changing rather than printing once? This is
> a **continuous** query over an unbounded stream — Flink emits an updated row each time an event
> changes a group's aggregate. That "update stream" is the streaming analog of re-running a batch
> `GROUP BY` every second.

---

## Exercise 3 — Windowed Aggregation (Real-Time Metrics)

> **What this shows:** the core of real-time reporting — bucketing events by time. A tumbling
> window counts claims per region per fixed interval, which is exactly "claims per minute per
> region" for a live dashboard.

```sql
SELECT
  window_start, window_end, region, COUNT(*) AS claims
FROM TABLE(
  TUMBLE(TABLE claims, DESCRIPTOR(event_time), INTERVAL '30' SECONDS)
)
GROUP BY window_start, window_end, region;
```

Each 30-second bucket emits one row per region once the window closes (driven by the watermark).
This is the shape of every "per hour / per 15 minutes" metric.

> **If a sharp student asks:** tumbling vs. hopping? Tumbling windows are fixed and
> non-overlapping (each event in exactly one bucket). A hopping window overlaps — e.g. "the last
> 1 minute, recomputed every 15 seconds" — using `HOP(...)`. Use tumbling for period totals,
> hopping for smooth rolling metrics.

---

## Exercise 4 — Continuous ETL: `INSERT INTO` a Kafka Sink

> **What this shows:** a running pipeline. `INSERT INTO` writes query results back to a Kafka
> topic continuously — this is how a live aggregate topic (feeding a dashboard) stays current
> without any application code.

### 4.1 Declare a sink table and start the pipeline

```sql
CREATE TABLE claims_per_region (
  region STRING,
  claim_count BIGINT,
  PRIMARY KEY (region) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'lab07-claims-per-region',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO claims_per_region
SELECT region, COUNT(*) FROM claims GROUP BY region;
```

This submits a **job** to Flink (see it running at <http://localhost:8082>). It keeps the
`lab07-claims-per-region` topic updated.

### 4.2 Read the live aggregate topic

In a host terminal:

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab07-claims-per-region --from-beginning --timeout-ms 8000 \
  --property print.key=true
```

You'll see per-region counts, updated as new claims arrive — a real-time reporting feed.

> **If a sharp student asks:** why `upsert-kafka` instead of `kafka`? The aggregate is *state*
> (the latest count per region), not an event log — a KTable, essentially. `upsert-kafka` writes
> keyed, compacted updates (new count replaces old for a region), so the topic holds current
> state rather than an ever-growing event history.

---

## Exercise 5 — Correlate Two Streams (Windowed Join)

> **What this shows:** the correlation use case — "for each claim, did this person also make
> contact within the hour?" A stream–stream join matches events from two topics by key inside a
> time window, which a bare consumer can't do cheaply.

### 5.1 Declare a second source and feed it

```sql
CREATE TABLE contacts (
  person_id  STRING,
  channel    STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'lab07-contacts',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'properties.group.id' = 'flink-contacts',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);
```

Feed a few contact events (reuse the CLI producer — some `person_id`s overlap with claims).

The timestamps **must line up with the claims you just fed**, because the join below only
matches within ±1 hour. `ClaimFeeder` stamps events with `Instant.now()`, which is UTC — so
generate the contact timestamps the same way rather than hardcoding a date:

```bash
TS=$(date -u '+%Y-%m-%d %H:%M:%S.000')
docker exec -i kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic lab07-contacts <<EOF
{"person_id":"person-1","channel":"PHONE","event_time":"$TS"}
{"person_id":"person-2","channel":"PHONE","event_time":"$TS"}
{"person_id":"person-3","channel":"WEB","event_time":"$TS"}
EOF
```

> **Note the unquoted `<<EOF`** — with `<<'EOF'` the shell would not substitute `$TS` and you
> would publish the literal text `$TS`, which fails to parse as a timestamp. If your join
> returns no rows, this is the first thing to check: the two streams must overlap in *event*
> time, not in the order you happened to run the commands.

### 5.2 Join claims to contacts by person, within a window

```sql
SELECT c.claim_id, c.person_id, c.region, k.channel
FROM claims  AS c
JOIN contacts AS k
  ON c.person_id = k.person_id
 AND c.event_time BETWEEN k.event_time - INTERVAL '1' HOUR
                      AND k.event_time + INTERVAL '1' HOUR;
```

The result shows only claims whose person also made contact within an hour — the "claim
submitted, and this person called" correlation. Stop with `Q`.

> **If a sharp student asks:** why must a stream–stream join be windowed? Because both sides are
> unbounded — without a time bound, Flink would have to keep *all* events from both streams
> forever to look for a future match. The window caps how long a row waits for a partner, which
> bounds the state.

---

## Review Questions

1. In Flink SQL, why does `SELECT region, COUNT(*) FROM claims GROUP BY region` keep emitting
   updated rows instead of returning once?
2. What is a **watermark**, and which kind of query can't work correctly without one?
3. You need "claims per office per 15 minutes" for a live dashboard. Which windowing construct do
   you use, and what makes each event land in exactly one bucket?
4. Why does the aggregate sink use `upsert-kafka` rather than the plain `kafka` connector? Relate
   your answer to KStream vs. KTable.
5. A stream–stream join requires a time bound in its `ON` clause. Explain why an unbounded join
   would be a problem, in terms of state.
6. When would you implement this logic in **Kafka Streams (Java)** inside a service instead of
   Flink SQL?

## What's Next

You can process and correlate streams in real time. The final module ties everything together:
**Module 11 (Reliability, Scaling & Operations for Developers)** and **Lab 08** — the capstone,
an end-to-end pipeline (ingest → process → sink) with reliability and monitoring.
