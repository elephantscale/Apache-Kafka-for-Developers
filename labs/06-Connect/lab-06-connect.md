# Lab 6 — Source & Sink Connectors

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 5 — Kafka Connect
- **Duration:** ~75 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Tools:** Connect REST API (curl), PostgreSQL, MinIO (S3), a small Java producer

## Objectives

By the end of this lab you will be able to:

- Start the Connect stack and deploy connectors via the REST API
- Run a JDBC **source** connector that streams a Postgres table into Kafka
- Run an S3 **sink** connector that lands a topic into object storage (MinIO)
- Manage connector lifecycle (status, pause, resume, restart)
- Configure error tolerance and a **Dead Letter Queue**, then inspect a routed bad record

## Prerequisites

- The core cluster running, plus the **connect profile**:
  ```bash
  docker compose up -d
  docker compose --profile connect up -d
  docker compose ps          # kafka-connect, postgres, minio, minio-setup present
  ```
- `curl` and `jq` on the host
- JDK 17 + Maven (for the DLQ producer in Exercise 5) — see [`labs/SETUP.md`](../SETUP.md)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster with the **connect** profile:
> Kafka Connect (REST at `http://localhost:8083`), PostgreSQL (`orders_db`), and MinIO (an
> S3-compatible store, console at `http://localhost:9001`, user/pass `minioadmin`). Connect runs
> in distributed mode and stores its state in Kafka (`connect-configs/offsets/status`). The
> lab cluster's Connect image installs the JDBC and S3 connector plugins on startup.

### Verify Connect is up and has the plugins

```bash
curl -s http://localhost:8083/ | jq .                    # cluster info
curl -s http://localhost:8083/connector-plugins | jq '.[].class' | grep -Ei 'jdbc|s3'
```

You should see `JdbcSourceConnector`, `JdbcSinkConnector`, and the S3 sink. (If the list is
empty, the plugin install is still running — give it a minute and re-check.)

---

## Exercise 1 — Prepare Source Data (PostgreSQL)

> **What this shows:** this builds the upstream "system of record" the JDBC source connector will
> poll. The `updated_at` timestamp and the monotonic `id` are exactly what the connector tracks
> to know "what have I already read."

### 1.1 Create and seed the orders table

```bash
docker exec -i postgres psql -U kafka_user -d orders_db <<'SQL'
CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    order_id    VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(16) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO orders (order_id, customer_id, amount, status) VALUES
  ('A-1001','cust-1', 42.00,'NEW'),
  ('A-1002','cust-2',108.50,'NEW'),
  ('A-1003','cust-1', 19.99,'NEW');
SQL
echo "seeded orders"
```

> **If a sharp student asks:** why does the connector need both `id` and `updated_at`? In
> `timestamp+incrementing` mode it uses the timestamp to catch **updated** rows and the id to
> disambiguate rows sharing a timestamp and to avoid missing/duplicating at boundaries. Id alone
> misses updates; timestamp alone can miss rows written in the same millisecond.

---

## Exercise 2 — Deploy a JDBC Source Connector

> **What this shows:** integration as configuration. You POST JSON; Connect starts polling
> Postgres and producing each row to a Kafka topic — no producer code, offsets tracked for you.

### 2.1 Create the connector

```bash
curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data '{
    "name": "orders-source",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "connection.url": "jdbc:postgresql://postgres:5432/orders_db",
      "connection.user": "kafka_user",
      "connection.password": "kafka_pw",
      "table.whitelist": "orders",
      "mode": "timestamp+incrementing",
      "timestamp.column.name": "updated_at",
      "incrementing.column.name": "id",
      "topic.prefix": "pg-",
      "poll.interval.ms": "2000",
      "numeric.mapping": "best_fit",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }' | jq .
```

> **Why `numeric.mapping`:** without it, `NUMERIC(10,2)` arrives as a Connect *Decimal*
> logical type, which JsonConverter renders as base64 — `"amount":"EGg="` instead of
> `"amount":42.00`. `best_fit` maps the column to a numeric JSON value instead. It is one of
> the most common "my data looks like garbage" surprises with the JDBC source.

### 2.2 Check status and read the topic

```bash
curl -s http://localhost:8083/connectors/orders-source/status | jq '.connector.state, .tasks[].state'

docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pg-orders --from-beginning --timeout-ms 6000
```

You'll see the three seeded rows as JSON on topic `pg-orders`.

### 2.3 Prove it's live

Insert a new row and watch it appear (the connector polls every 2s):

```bash
docker exec -i postgres psql -U kafka_user -d orders_db \
  -c "INSERT INTO orders (order_id, customer_id, amount, status) VALUES ('A-1004','cust-3',75.00,'NEW');"

docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pg-orders --from-beginning --timeout-ms 6000 | tail -1
```

> **If a sharp student asks:** if I DELETE a row, does it disappear from Kafka? No — a JDBC poll
> source only runs `SELECT`, so it never sees deletes (and the already-produced record stays in
> the topic). For deletes you need log-based CDC (Debezium). This is the poll-source blind spot.

---

## Exercise 3 — Deploy an S3 Sink Connector (MinIO)

> **What this shows:** the other direction. A sink connector is a consumer group that batches
> records into files in object storage — the "land it in the data lake" pattern, here to MinIO.

### 3.1 Create the sink

```bash
curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data '{
    "name": "orders-s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "topics": "pg-orders",
      "s3.bucket.name": "kafka-data-lake",
      "s3.region": "us-east-1",
      "store.url": "http://minio:9000",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "flush.size": "3",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }' | jq .
```

### 3.2 Verify objects landed in MinIO

Open the MinIO console at <http://localhost:9001> (login `minioadmin` / `minioadmin`) and browse
the `kafka-data-lake` bucket — you'll see JSON objects under `topics/pg-orders/…`. (`flush.size:3`
means a file is written every 3 records.)

> **If a sharp student asks:** is this sink exactly-once? It's at-least-once by default; on retry
> a batch could be rewritten. The S3 sink achieves effectively-once by using deterministic file
> names keyed on Kafka offsets, so a re-flush overwrites rather than duplicates — idempotency at
> the destination, exactly the Module 7 boundary.

---

## Exercise 4 — Manage the Connector Lifecycle

> **What this shows:** Connect is operated entirely over REST. Pause/resume/restart and status
> are how you run connectors day to day.

```bash
# list all connectors
curl -s http://localhost:8083/connectors | jq .

# pause, confirm, resume
curl -s -X PUT http://localhost:8083/connectors/orders-source/pause
curl -s http://localhost:8083/connectors/orders-source/status | jq '.connector.state'   # PAUSED
curl -s -X PUT http://localhost:8083/connectors/orders-source/resume

# restart a failed task (task 0)
curl -s -X POST http://localhost:8083/connectors/orders-source/tasks/0/restart
```

> **If a sharp student asks:** where did the connector config and offsets go — are they lost if
> Connect restarts? No. They're stored in the `connect-configs` and `connect-offsets` Kafka
> topics, so a worker restart resumes from the same place. That's why distributed-mode workers
> are stateless.

---

## Exercise 5 — Error Tolerance and a Dead Letter Queue

> **What this shows:** the operational feature that keeps pipelines alive. With a DLQ, a poison
> record is routed aside (with headers explaining why) instead of stopping the task — good
> records keep flowing.

### 5.1 Create a topic and a DLQ-enabled sink

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab06-events --partitions 3 --replication-factor 3
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic dlq-events --partitions 3 --replication-factor 3

curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data '{
    "name": "events-s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "topics": "lab06-events",
      "s3.bucket.name": "kafka-data-lake",
      "s3.region": "us-east-1",
      "store.url": "http://minio:9000",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "flush.size": "1",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "errors.tolerance": "all",
      "errors.deadletterqueue.topic.name": "dlq-events",
      "errors.deadletterqueue.topic.replication.factor": "3",
      "errors.deadletterqueue.context.headers.enable": "true"
    }
  }' | jq .
```

The JSON converter will fail to parse non-JSON values — those become our "bad" records.

### 5.2 Inject good and bad records (Java producer)

Add this to your Maven project (from Lab 05), or a fresh `lab06/` project with the same `pom.xml`:

```java
// src/main/java/com/elephantscale/kafka/DlqInjector.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class DlqInjector {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      // good JSON records
      for (int i = 0; i < 5; i++) {
        producer.send(new ProducerRecord<>("lab06-events", "k" + i,
            "{\"id\": " + i + ", \"ok\": true}"));
      }
      // poison records — NOT valid JSON, the sink's JsonConverter will reject these
      producer.send(new ProducerRecord<>("lab06-events", "bad1", "this is not json"));
      producer.send(new ProducerRecord<>("lab06-events", "bad2", "{broken:"));
      producer.flush();
    }
    System.out.println("injected 5 good + 2 bad records");
  }
}
```

```bash
cd lab06 && mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.DlqInjector
```

### 5.3 Inspect the DLQ

The good records land in MinIO; the two bad ones are routed to `dlq-events` — and the sink task
stays `RUNNING`.

> **Note `flush.size: 1` here.** `flush.size` counts records **per partition**, not per topic.
> `lab06-events` has 3 partitions, so 5 good records land roughly 2/2/1 — with `flush.size: 5`
> no partition ever reaches the threshold and *nothing* is written, which looks like a broken
> sink. Exercise 3 got away with `flush.size: 3` because the JDBC source writes all rows to a
> single partition. In production you pair a realistic `flush.size` with `rotate.interval.ms`
> so partial batches still get flushed on a timer.

> **Note `key.converter` too.** The sink sets it explicitly to `StringConverter` because these
> records have plain string keys (`k0`, `k1`, …). Without it the key falls back to the worker
> default — `JsonConverter` — which cannot parse `k0`, so *every* record fails on its key and
> the DLQ fills with your good records as well as the bad. If you see healthy records in the
> DLQ, check the `__connect.errors.stage` header: `KEY_CONVERTER` is the giveaway.

```bash
curl -s http://localhost:8083/connectors/events-s3-sink/status | jq '.tasks[].state'   # RUNNING

# read the DLQ with headers (the headers explain WHY each record failed)
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic dlq-events --from-beginning --timeout-ms 6000 \
  --property print.headers=true
```

The header set includes the failing connector, the exception class, and the error message —
enough to triage and reprocess.

> **If a sharp student asks:** without `errors.tolerance=all`, what would have happened? The
> first bad record would have thrown and put the task in `FAILED` — stopping the whole pipeline
> on one poison message. The DLQ is exactly the difference between "one bad record halts
> everything" and "one bad record gets set aside."

### 5.4 Clean up (optional)

```bash
for c in orders-source orders-s3-sink events-s3-sink; do
  curl -s -X DELETE http://localhost:8083/connectors/$c; done
```

---

## Review Questions

1. In Connect's model, what is the difference between a **connector** and a **task**, and what
   determines how many tasks a sink connector can usefully run?
2. Connect workers are described as "stateless." Where does connector configuration and source
   progress actually live, and why does that make workers stateless?
3. Your JDBC source isn't capturing row **deletes**. Explain why, and what technology you'd use
   instead.
4. A single malformed record keeps stopping your sink task. Which two config settings turn that
   into a "route it aside and keep going" behavior, and where do the bad records end up?
5. Is the S3 sink exactly-once? Explain the mechanism that keeps re-flushed batches from
   duplicating data in the bucket.
6. When would you reach for a Single Message Transform, and when is the job really a
   stream-processing task instead?

## What's Next

You can move data in and out of Kafka without code. Next you'll **process** streams in flight:
**Module 10 (Stream Processing)** and **Lab 07** — Kafka Streams and **Flink SQL**: stateless vs.
stateful operations, joins, windowed aggregations, and exactly-once processing (uses the `flink`
compose profile).
