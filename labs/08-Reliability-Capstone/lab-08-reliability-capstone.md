# Lab 8 — Reliability & End-to-End Capstone

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 7 — Reliability, Scaling & Operations
- **Duration:** ~90 minutes
- **Difficulty:** Advanced (capstone)
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Tools:** Java producer, Flink SQL, Kafka Connect, Prometheus/Grafana

## Objectives

By the end of this capstone you will be able to:

- Configure a topic and producer for the full durability contract (RF, `acks=all`,
  `min.insync.replicas`)
- Build an end-to-end pipeline: **ingest → process → sink**
- Monitor consumer lag in Grafana and drain it by scaling consumers
- Kill a broker under load and prove the pipeline survives with no data loss

## Prerequisites

- The core cluster, plus the **flink**, **connect**, and **monitoring** profiles:
  ```bash
  docker compose up -d
  docker compose --profile connect --profile flink --profile monitoring up -d
  docker compose ps
  ```
- Labs 02, 06, and 07 completed (producer, Connect, Flink SQL) — this capstone reuses those skills
- JDK 17 + Maven; Grafana reachable at <http://localhost:3000> (anonymous access is enabled)

## Lab Environment

> This capstone runs the full stack on the local **Docker Compose** cluster: 3 KRaft brokers,
> Schema Registry, Kafka Connect + MinIO, Flink, and Prometheus/Grafana. You'll ingest with a
> Java producer, process with Flink SQL, sink to MinIO with Connect, and watch lag in Grafana —
> then kill a broker to prove reliability. It is deliberately integrative: little new material,
> everything connected.

### The pipeline

```
  Java producer  ──►  lab08-claims  ──►  Flink SQL  ──►  lab08-claims-per-region  ──►  S3 sink  ──►  MinIO
   (acks=all,          (RF=3,             (windowed                                     (Connect)
    idempotent)         min.isr=2)         aggregation)
                              └──────────  monitor lag in Grafana  ──────────┘
                              └──────────  kill a broker, survive  ──────────┘
```

### Create topics with the durability contract

```bash
# input + output topics, RF=3
for t in lab08-claims lab08-claims-per-region; do
  docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $t --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2
done

docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic lab08-claims
```

`min.insync.replicas=2` is set at the topic level — the durability floor.

---

## Exercise 1 — Durable Ingest

> **What this shows:** the producer side of the durability contract. With `acks=all` +
> `enable.idempotence=true` against a topic with `min.insync.replicas=2`, an acknowledged write is
> safely on at least two brokers, with no duplicates from retries.

### 1.1 The ingest producer

```java
// src/main/java/com/elephantscale/kafka/CapstoneIngest.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class CapstoneIngest {
  public static void main(String[] args) throws Exception {
    int total = args.length > 0 ? Integer.parseInt(args[0]) : 2000;
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG, "all");                 // durability
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);    // no duplicate retries
    p.put(ProducerConfig.LINGER_MS_CONFIG, 10);               // a little batching
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

    String[] regions = {"EAST", "WEST", "SOUTH"};
    Random r = new Random();
    long ok = 0;
    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 1; i <= total; i++) {
        String region = regions[r.nextInt(regions.length)];
        String person = "person-" + r.nextInt(50);
        String ts = Instant.now().toString().replace("Z","").substring(0,23).replace("T"," ");
        String json = String.format(
          "{\"claim_id\":%d,\"person_id\":\"%s\",\"region\":\"%s\",\"amount\":%.2f,\"event_time\":\"%s\"}",
          i, person, region, 10 + r.nextInt(490) + r.nextDouble(), ts);
        producer.send(new ProducerRecord<>("lab08-claims", person, json),
          (md, ex) -> { if (ex != null) System.out.println("SEND FAILED: " + ex); });
        ok++;
        if (i % 200 == 0) { producer.flush(); System.out.println("sent " + i); }
        Thread.sleep(20);   // ~50/sec, slow enough to watch
      }
      producer.flush();
    }
    System.out.println("ingest complete, acknowledged=" + ok);
  }
}
```

Compile it now; you'll run it in Exercise 4 (under failure) and here for a smoke test:

```bash
cd labs/kafka-labs
./run.sh CapstoneIngest 200
```

> **If a sharp student asks:** what happens to a `send` if two brokers are down (ISR < min.isr)?
> The broker returns `NOT_ENOUGH_REPLICAS`; the idempotent producer retries, and if it can't
> satisfy `min.insync.replicas` the callback surfaces the error rather than acknowledging an
> unsafe write. That's the contract doing its job — fail rather than lose data.

---

## Exercise 2 — Process (Flink SQL)

> **What this shows:** the processing stage — a windowed aggregate that turns raw claims into a
> live per-region metric, written back to Kafka for the sink and dashboard to consume.

In the Flink SQL client (`docker exec -it flink-jobmanager ./bin/sql-client.sh`):

```sql
CREATE TABLE claims (
  claim_id INT, person_id STRING, region STRING, amount DOUBLE, event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'='kafka', 'topic'='lab08-claims',
  'properties.bootstrap.servers'='kafka-1:9092',
  'properties.group.id'='flink-capstone',
  'format'='json', 'scan.startup.mode'='earliest-offset'
);

CREATE TABLE claims_per_region (
  region STRING, claim_count BIGINT, PRIMARY KEY (region) NOT ENFORCED
) WITH (
  'connector'='upsert-kafka', 'topic'='lab08-claims-per-region',
  'properties.bootstrap.servers'='kafka-1:9092',
  'key.format'='json', 'value.format'='json'
);

INSERT INTO claims_per_region
SELECT region, COUNT(*) FROM claims GROUP BY region;
```

This submits a continuous Flink job (visible at <http://localhost:8082>) that keeps
`lab08-claims-per-region` current.

---

## Exercise 3 — Sink (Kafka Connect → MinIO) and Monitor (Grafana)

> **What this shows:** the sink stage lands results in object storage; Grafana shows the pipeline's
> health. Together they're the "reporting output + operational visibility" an SSA-style analytics
> POC needs.

### 3.1 Sink the aggregate topic to MinIO

```bash
curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data '{
    "name": "capstone-s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "topics": "lab08-claims-per-region",
      "s3.bucket.name": "kafka-data-lake",
      "s3.region": "us-east-1",
      "store.url": "http://minio:9000",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "flush.size": "10",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }' | jq '.name'
```

### 3.2 Open Grafana and find consumer lag

- Grafana: <http://localhost:3000> → the provisioned **Kafka** dashboard
- Watch **consumer lag** for the Flink and Connect consumer groups as the pipeline runs
- Also available from the CLI:

```bash
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group flink-capstone
```

> **If a sharp student asks:** which lag matters most here? The processing stage's — if the Flink
> job's group lag climbs, your aggregates (and the dashboard) fall behind real time. That's the
> number to alert on for a real-time reporting pipeline.

---

## Exercise 4 — Prove Reliability: Kill a Broker Under Load

> **What this shows:** the payoff of the durability contract. With RF 3 and `min.insync.replicas=2`,
> killing one broker triggers leader election and a brief client retry — but no acknowledged data
> is lost and the pipeline keeps running.

### 4.1 Start a heavier ingest, then kill a broker

```bash
# terminal A — ingest 2000 records (~40s)
./run.sh CapstoneIngest 2000
```

```bash
# terminal B — a few seconds in, kill one broker
docker kill kafka-2
# watch leadership move
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic lab08-claims        # note new leaders; ISR now {1,3} for some partitions
```

The producer may log a brief retry, then continue. Ingest completes with
`acknowledged=2000`.

### 4.2 Restore and verify no loss

```bash
docker start kafka-2                      # broker rejoins, catches up
sleep 20
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic lab08-claims        # ISR back to {1,2,3}

# count what actually landed (should equal everything you have ingested into this topic)
docker exec kafka-1 kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic lab08-claims | \
  awk -F: '{sum+=$3} END {print "total records:", sum}'
```

The total equals everything the producer ever acknowledged on this topic — 2200 if you ran
Exercise 1's 200 and then this 2000. A broker died mid-stream and **not one acknowledged
record was lost**.

> **If the count comes back 0:** you're on the old command. `kafka.tools.GetOffsetShell` was
> removed in Kafka 4 (the tools moved to `org.apache.kafka.tools`), and
> `kafka-run-class.sh` on a missing class prints nothing and exits 0 — so the `awk` sums an
> empty stream and reports `0`, which reads as "we lost everything". Use `kafka-get-offsets.sh`.

> **If a sharp student asks:** what if I'd killed two brokers? With `min.insync.replicas=2` and
> RF 3, losing two brokers drops the ISR below the floor, so producers get
> `NOT_ENOUGH_REPLICAS` and **writes pause** — deliberately. Kafka refuses to acknowledge writes
> it can't make durable. Availability yields to durability; that's the trade you configured.

---

## Exercise 5 — Drain Lag by Scaling (Capstone Wrap)

> **What this shows:** the operational reflex from the module — when a consumer group lags, scale
> it toward the partition count and watch lag drain.

Build the backlog **before** any consumer is running — on a laptop a single console consumer
drains 5000 small records as fast as you can produce them, so "watch lag climb" shows you a
flat zero. Create the lag first, then scale into it:

```bash
# 1. register the group and stop it immediately, so it has committed offsets to lag behind
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab08-claims --group capstone-drain --from-beginning --timeout-ms 10000 > /dev/null

# 2. burst with the group idle -- this is what builds the backlog
./run.sh CapstoneIngest 5000

# 3. look at the lag you just created
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group capstone-drain
```

You'll see roughly 5000 spread across the three partitions. Now scale into it — run this in
three separate terminals (3 = the partition count) and re-run the `--describe` as they work:

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab08-claims --group capstone-drain
```

Lag drains to 0. Then start a **fourth** consumer in the same group and re-run `--describe`:
it is assigned no partitions and sits idle — the partition ceiling.

> **If a sharp student asks:** the fourth consumer is idle — is that wasted? Yes, for this topic:
> a group can't have more active consumers than partitions. To scale past 3 you'd need more
> partitions — which is exactly why partition count is a capacity decision you make up front.

---

## Review Questions

1. Write the three settings that make up the durability contract and state, for RF 3, how many
   broker failures you can tolerate while still accepting writes.
2. In Exercise 4 you killed a broker and lost no acknowledged data. Name the two mechanisms
   (one broker-side, one client-side) that made that possible.
3. Your real-time dashboard is falling behind. Which consumer group's lag do you check first in
   this pipeline, and why that one?
4. A single consumer can't keep up with `lab08-claims`. What's your first scaling move, and what
   caps it?
5. During a two-broker outage your producer starts throwing `NOT_ENOUGH_REPLICAS`. Is this a bug?
   Explain in terms of `min.insync.replicas`.
6. Sketch this pipeline end to end (ingest → process → sink → monitor) and name the Kafka feature
   that provides each stage's reliability.

## Congratulations — Course Complete

You've gone from "what is Kafka" to a durable, monitored, end-to-end streaming pipeline you built
and stress-tested yourself:

- **Ingest** with a tuned, idempotent, `acks=all` producer
- **Process** in real time with Flink SQL (windowed aggregation)
- **Sink** to object storage with Kafka Connect
- **Monitor** consumer lag and **survive** a broker failure with no data loss

Everything is in the Git repository — re-run and adapt it on your own projects. This is the
foundation for building production streaming systems.
