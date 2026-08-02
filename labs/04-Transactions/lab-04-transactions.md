# Lab 4 — Delivery Semantics & Transactions

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 3 — Delivery Semantics & Transactions
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate / Advanced
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Language:** Java (Kafka Java client, Maven)

## Objectives

By the end of this lab you will be able to:

- Use a transactional producer (`transactional.id`, `initTransactions`, begin/commit/abort)
- See how an aborted transaction is hidden from a `read_committed` consumer
- Build a consume-process-produce pipeline that commits input offsets *inside* the transaction
- Prove the pipeline is exactly-once by crashing it mid-batch and restarting
- Explain the boundary: exactly-once inside Kafka vs. idempotent external side effects

## Prerequisites

- The core cluster running (`docker compose up -d`, three brokers healthy)
- **JDK 17** and **Maven** on the host (see the Java project setup below)
- Labs 02–03 completed (idempotent producer; manual-commit consumer)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no ZooKeeper,
> no Kubernetes). Your code is a Maven project that runs on the host and connects to
> `localhost:9092`. The transaction-state internal topic is replicated across all three brokers
> (`transaction.state.log.replication.factor=3`, `min.isr=2`) — already configured in the
> lab compose file, so transactions work out of the box.

### Java project setup

Create a project folder `lab04/` with this `pom.xml` (same pattern as the other Java labs):

```xml
<!-- lab04/pom.xml -->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.elephantscale.kafka</groupId>
  <artifactId>lab04</artifactId>
  <version>1.0</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>4.0.2</version>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
    </dependency>
    <!-- kafka-clients pulls slf4j-api 1.7.36 transitively; pin the 2.x API so it
         matches the 2.x binding below. Mismatched, SLF4J prints a StaticLoggerBinder
         warning on every run and silently disables all client logging. -->
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>2.0.13</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.13</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <!-- run a class with: mvn -q exec:java -Dexec.mainClass=... -->
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>3.1.0</version>
      </plugin>
    </plugins>
  </build>
</project>
```

Put Java sources under `lab04/src/main/java/com/elephantscale/kafka/`.

### Create the lab topics

```bash
for t in lab04-input lab04-output; do
  docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $t --partitions 3 --replication-factor 3
done
```

Seed some input:

```java
// save as SeedInput.java  — usage: mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.SeedInput -Dexec.args="20"
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SeedInput {
  public static void main(String[] args) {
    int n = args.length > 0 ? Integer.parseInt(args[0]) : 20;
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < n; i++) {
        String value = String.format("{\"id\": %d, \"amount\": %d}", i, 10 + i);
        producer.send(new ProducerRecord<>("lab04-input", "acct-" + (i % 4), value));
      }
      producer.flush();
    }
    System.out.println("seeded " + n + " input records");
  }
}
```

```bash
cd lab04
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.SeedInput -Dexec.args="20"
```

---

## Exercise 1 — A Transactional Producer

> **What this shows:** the transactional producer lifecycle. A stable `transactional.id`
> gives the producer a durable identity; `initTransactions()` registers it; then work is
> wrapped in `beginTransaction()` / `commitTransaction()`. Records become visible to
> `read_committed` readers only at commit.

### 1.1 Commit a transaction

```java
// save as TxnProducer.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class TxnProducer {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lab04-txn-producer");  // stable identity
    // enable.idempotence is implied by transactional.id

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      producer.initTransactions();
      producer.beginTransaction();
      try {
        for (int i = 0; i < 5; i++) {
          String value = String.format("{\"id\": %d, \"status\": \"CONFIRMED\"}", i);
          producer.send(new ProducerRecord<>("lab04-output", "acct-" + i, value));
        }
        producer.commitTransaction();
        System.out.println("committed 5 records in one transaction");
      } catch (Exception e) {
        producer.abortTransaction();
        System.out.println("aborted: " + e);
      }
    }
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.TxnProducer
```

### 1.2 Read them back (committed)

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed
```

All five appear — they were committed atomically.

> **If a sharp student asks:** what does `initTransactions()` actually do? It registers the
> `transactional.id` with the transaction coordinator and **fences** any earlier producer
> instance using the same id (bumping an epoch), so a zombie predecessor can no longer commit.
> It also recovers/aborts any in-flight transaction left by a crashed prior run.

---

## Exercise 2 — Abort Is Invisible to `read_committed`

> **What this shows:** an aborted transaction's records physically exist in the log, but a
> `read_committed` consumer never sees them, while a `read_uncommitted` consumer does. This is
> the mechanism that makes exactly-once safe on the read side.

### 2.1 Produce a committed batch and an aborted batch

```java
// save as TxnAbort.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class TxnAbort {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lab04-abort-demo");

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      producer.initTransactions();

      // batch 1 — COMMIT
      producer.beginTransaction();
      for (int i = 0; i < 3; i++) {
        producer.send(new ProducerRecord<>("lab04-output",
            String.format("{\"batch\": 1, \"i\": %d}", i)));
      }
      producer.commitTransaction();
      System.out.println("committed batch 1");

      // batch 2 — ABORT
      producer.beginTransaction();
      for (int i = 0; i < 3; i++) {
        producer.send(new ProducerRecord<>("lab04-output",
            String.format("{\"batch\": 2, \"i\": %d}", i)));
      }
      producer.flush();              // force these records to the broker BEFORE aborting,
                                     // so there is really something on disk to hide
      producer.abortTransaction();
      System.out.println("aborted batch 2");
    }
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.TxnAbort
```

### 2.2 Compare the two isolation levels

```bash
echo "=== read_committed (should NOT see batch 2) ==="
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep '"batch": 2' && echo "LEAK!" || echo "batch 2 correctly hidden"

echo "=== read_uncommitted (WILL see batch 2) ==="
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_uncommitted | grep -c '"batch": 2'
```

`read_committed` hides the aborted batch; `read_uncommitted` shows it.

> **If a sharp student asks:** if aborted records are still written to disk, isn't that waste?
> A little — the broker writes the records plus an abort marker, and compaction/retention
> eventually reclaim them. The design keeps the append-only log simple: nothing is ever
> rewritten in place; readers just filter using the markers.

---

## Exercise 3 — Consume-Process-Produce (Exactly-Once)

> **What this shows:** the canonical exactly-once pipeline. The consumer does **not**
> auto-commit; instead the producer binds the input offsets into the same transaction as the
> output, so outputs and the offset advance commit atomically.

### 3.1 The pipeline

```java
// save as PipelineEos.java
package com.elephantscale.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PipelineEos {
  public static void main(String[] args) {
    int crashAfter = args.length > 0 ? Integer.parseInt(args[0]) : -1;   // -1 = never crash
    ObjectMapper mapper = new ObjectMapper();

    Properties cp = new Properties();
    cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    cp.put(ConsumerConfig.GROUP_ID_CONFIG, "lab04-eos");
    cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // offsets committed ONLY via the transaction
    cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Properties pp = new Properties();
    pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lab04-eos-pipeline");

    Consumer<String, String> consumer = new KafkaConsumer<>(cp);
    Producer<String, String> producer = new KafkaProducer<>(pp);
    consumer.subscribe(List.of("lab04-input"));
    producer.initTransactions();

    int processed = 0;
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, String> msg : records) {
        producer.beginTransaction();
        try {
          JsonNode record = mapper.readTree(msg.value());
          int id = record.get("id").asInt();
          double amount = record.get("amount").asDouble();
          String enriched = String.format("{\"id\": %d, \"amount\": %s, \"tax\": %s}",
              id, amount, amount * 0.1);
          producer.send(new ProducerRecord<>("lab04-output", String.valueOf(id), enriched));

          // bind THIS input offset into the transaction
          Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
              new TopicPartition(msg.topic(), msg.partition()),
              new OffsetAndMetadata(msg.offset() + 1));
          producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

          // optional fault injection BEFORE commit
          processed++;
          if (processed == crashAfter) {
            System.out.println("CRASH before committing record " + id);
            Runtime.getRuntime().halt(1);
          }

          producer.commitTransaction();
          System.out.println("committed id=" + id + "  (processed=" + processed + ")");
        } catch (Exception e) {
          producer.abortTransaction();
          System.out.println("aborted: " + e);
        }
      }
    }
  }
}
```

### 3.2 Run it clean

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.SeedInput -Dexec.args="20"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.PipelineEos   # Ctrl-C once "committed" lines stop appearing
```

Check the output count:

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep -c '"tax"'
```

---

## Exercise 4 — Prove Exactly-Once Under a Crash

> **What this shows:** the payoff. Crash the pipeline *after producing output but before
> committing* the transaction. Because output + offset commit are atomic, the aborted output
> is invisible and the input offset never advanced — on restart the record is reprocessed and
> the downstream sees it **exactly once**, not twice.

### 4.1 Reset and crash mid-stream

Use a fresh output topic so the count is unambiguous:

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic lab04-output
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab04-output --partitions 3 --replication-factor 3

# fresh consumer group so we read all 20 inputs from the start
```

Edit `PipelineEos.java`'s `group.id` to `lab04-eos-crash`, then:

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.SeedInput -Dexec.args="20"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.PipelineEos -Dexec.args="5"  # crashes right before committing the 5th record
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.PipelineEos                  # restart; runs to completion, Ctrl-C when idle
```

### 4.2 Count the output

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab04-output --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed | grep -c '"tax"'
```

You should get **exactly 20** — not 24. The 5th record's aborted output was discarded and its
offset never advanced, so the restart reprocessed it once. No duplicates, no loss.

> **If a sharp student asks:** what if it crashes *after* `commitTransaction()` returns but
> before the next poll? Nothing is lost — the offset was committed inside the transaction, so
> the restart simply resumes at the next record. The atomic unit is exactly "outputs + offset,"
> which is why neither ordering of the crash produces a duplicate.

---

## Exercise 5 — The Boundary (Discussion + Mini-Demo)

> **What this shows:** exactly-once is a guarantee *within Kafka*. A side effect to an external
> system (DB, email, REST) is **not** part of the Kafka transaction — so you make those
> idempotent instead.

Consider adding an external write to the pipeline:

```java
// INSIDE the try, alongside send():
//   db.insert(enriched);         // <-- NOT in the Kafka transaction!
```

Discuss before moving on:

1. If the transaction aborts *after* `db.insert()` ran, what's now inconsistent between the DB
   and the Kafka output?
2. Rewrite `db.insert(enriched)` as an idempotent operation so a reprocess is harmless. (Hint:
   upsert on a natural key like `id`.)
3. Why can't Kafka simply "include" the database write in its transaction?

> **If a sharp student asks:** isn't there Kafka-to-DB exactly-once with connectors? Some sink
> connectors achieve effectively-once by tracking offsets in the destination or using
> idempotent upserts — but that's the connector implementing idempotency at the edge, not the
> Kafka transaction reaching into the database. The boundary still holds.

---

## Review Questions

1. What two things does a transaction make atomic in a consume-process-produce loop, and why
   must the consumer have `enable.auto.commit=false`?
2. What is the `transactional.id` for, and what failure does producer "fencing" prevent?
3. A downstream team reads your transactional output but still sees duplicated/aborted records.
   What single consumer setting did they most likely miss?
4. In Exercise 4 you got exactly 20 outputs after a mid-stream crash. Explain, in terms of the
   atomic unit, why it wasn't 24.
5. Your pipeline also writes each result to PostgreSQL. Does Kafka's exactly-once cover that
   write? What technique makes the overall effect correct?
6. When would you deliberately choose at-least-once over exactly-once, despite the duplicates?

## What's Next

You've mastered correctness for raw bytes. Next, correctness for **data shape**:
**Module 8 (Serialization & the Schema Registry)** and **Lab 05** — Avro/Protobuf/JSON Schema,
registering schemas, schema IDs and serdes, and evolving a schema without breaking consumers.
