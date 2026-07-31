# Lab 2 — Producer Internals & Tuning

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 1 — Producer Internals
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Language:** Java (Kafka Java client, Maven)

## Objectives

By the end of this lab you will be able to:

- Write a Java producer with the Kafka client and confirm delivery via a `Callback`
- Control partitioning with message keys and observe per-key ordering
- Tune batching (`linger.ms`, `batch.size`) and compression, and measure the effect
- Choose an `acks` level and reason about its durability trade-off
- Enable the idempotent producer and understand what it does — and doesn't — guarantee

## Prerequisites

- The lab environment from [`labs/SETUP.md`](../SETUP.md); the core cluster running
  (`docker compose up -d`, all three brokers healthy)
- **JDK 17** and **Maven** on the host:
  ```bash
  java -version    # 17.x
  mvn -version     # 3.9+
  ```
- Lab 01 completed (you can create topics and read consumer lag)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no
> ZooKeeper, no Kubernetes). Your code is a Maven project that runs on the **host** and
> connects to `localhost:9092`. Kafka CLI tools run inside the brokers via
> `docker exec kafka-1 …`.

### Java project setup

Create a project folder `lab02/` with this `pom.xml`, and put sources under
`lab02/src/main/java/com/elephantscale/kafka/`:

```xml
<!-- lab02/pom.xml -->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.elephantscale.kafka</groupId>
  <artifactId>lab02</artifactId>
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

### Create the lab topic

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab02-orders --partitions 3 --replication-factor 3
```

---

## Exercise 1 — A First Producer with Delivery Reports

> **What this shows:** `send()` is asynchronous — it queues a record and returns
> immediately. You only learn the outcome (final partition/offset, or an error) from the
> **delivery callback**, which the client fires from a background thread once the broker
> acknowledges; `flush()` blocks until all in-flight sends complete.

### 1.1 Write the producer

```java
// save as ProducerBasic.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerBasic {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < 10; i++) {
        String value = String.format("{\"order_id\": %d, \"status\": \"PLACED\"}", i);
        ProducerRecord<String, String> record =
            new ProducerRecord<>("lab02-orders", "user-" + (i % 3), value);   // 3 distinct keys
        producer.send(record, (RecordMetadata md, Exception err) -> {
          if (err != null) {
            System.out.println("FAILED: " + err);
          } else {
            System.out.printf("ok  %s[%d] @ offset %d%n", md.topic(), md.partition(), md.offset());
          }
        });
      }
      producer.flush();   // block until all deliveries complete (callbacks fire)
    }
    System.out.println("done");
  }
}
```

### 1.2 Run it

```bash
cd lab02
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerBasic
```

You'll see ten `ok …[partition] @ offset …` lines. Note that keys `user-0/1/2` map to
specific partitions.

> **If a sharp student asks:** when do the callbacks fire? The client delivers each callback
> from its background I/O thread once the broker acknowledges that record — not on the calling
> thread. `flush()` (or closing the producer, as the try-with-resources does here) blocks until
> every in-flight send has completed and its callback has run, so no delivery is lost when the
> program exits.

### 1.3 Confirm from the CLI

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic lab02-orders --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true
```

---

## Exercise 2 — Keys, Partitioning, and Ordering

> **What this shows:** the key determines the partition (`hash(key) % partitions`), and a
> key is the *only* way to guarantee ordering for an entity. Keyless records spread across
> partitions and have no cross-partition order.

### 2.1 Prove same-key → same-partition

```java
// save as ProducerKeys.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerKeys {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // key -> set of partitions it landed on
    Map<String, Set<Integer>> seen = new ConcurrentHashMap<>();

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < 30; i++) {
        String key = "user-" + (i % 3);
        producer.send(new ProducerRecord<>("lab02-orders", key, "event-" + i),
          (md, err) -> {
            if (err == null) {
              seen.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(md.partition());
            }
          });
      }
      producer.flush();
    }

    new TreeMap<>(seen).forEach((key, partitions) ->
      System.out.println(key + " -> partition(s) " + new TreeSet<>(partitions)));
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerKeys
```

Each key prints **exactly one** partition — no key ever spans partitions.

### 2.2 Now go keyless

Change the `send(...)` to drop the key (pass `null`), and bucket by value instead of key:

```java
        final int n = i;
        producer.send(new ProducerRecord<>("lab02-orders", null, "event-" + i),
          (md, err) -> {
            if (err == null) {
              seen.computeIfAbsent("event-" + n, k -> ConcurrentHashMap.newKeySet()).add(md.partition());
            }
          });
```

Re-run and observe that keyless records are **spread across all three partitions**.

> **If a sharp student asks:** two different keys landed on the same partition — is that a
> bug? No. Keys are hashed into 3 buckets, so distinct keys can collide onto one partition.
> The guarantee is one-directional: one key never *splits* across partitions.

---

## Exercise 3 — Batching, Linger, and Compression

> **What this shows:** batching is where producer throughput comes from. `linger.ms` lets
> the producer wait a few milliseconds to fill larger batches; compression then shrinks
> those batches over the network and on disk. You'll measure the difference.

### 3.1 A throughput harness

```java
// save as ProducerThroughput.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerThroughput {
  public static void main(String[] args) {
    // args: <linger.ms> <compression>   e.g. "0 none" or "20 zstd"
    int linger = args.length > 0 ? Integer.parseInt(args[0]) : 0;
    String compression = args.length > 1 ? args[1] : "none";

    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.LINGER_MS_CONFIG, linger);
    p.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    p.put(ProducerConfig.ACKS_CONFIG, "all");

    int n = 100_000;
    String payload = "{\"data\": \"" + "x".repeat(200) + "\"}";   // ~200-byte records

    long start = System.currentTimeMillis();
    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < n; i++) {
        producer.send(new ProducerRecord<>("lab02-orders", "k-" + (i % 1000), payload));
      }
      producer.flush();
    }
    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
    System.out.printf("linger.ms=%-3d compression=%-6s %d records in %.2fs  ->  %,.0f rec/s%n",
        linger, compression, n, elapsed, n / elapsed);
  }
}
```

### 3.2 Compare configurations

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerThroughput -Dexec.args="0 none"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerThroughput -Dexec.args="20 none"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerThroughput -Dexec.args="20 zstd"
```

Compare the `rec/s`. Typically `linger.ms=20` beats `0`, and `zstd` on top adds more —
larger, compressed batches make far better use of the network.

> **If a sharp student asks:** doesn't `linger.ms=20` add 20ms of latency to every record?
> At most 20ms, and only when the batch isn't already full. Under load, batches fill before
> the timer expires, so you get the throughput win with negligible added latency. Under a
> trickle, you pay up to 20ms — usually a fine trade.

---

## Exercise 4 — Acks and Durability

> **What this shows:** `acks` is the durability dial. You'll see that all three levels
> "work" against a healthy cluster — the difference only appears under failure — so the
> choice is about what you're willing to lose, not about whether it runs.

### 4.1 Time the acks levels

```java
// save as ProducerAcks.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerAcks {
  public static void main(String[] args) {
    String acks = args.length > 0 ? args[0] : "all";   // 0 | 1 | all

    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG, acks);

    int n = 50_000;
    long start = System.currentTimeMillis();
    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < n; i++) {
        producer.send(new ProducerRecord<>("lab02-orders", "k-" + i, "v-" + i));
      }
      producer.flush();
    }
    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
    System.out.printf("acks=%-3s %d records in %.2fs%n", acks, n, elapsed);
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerAcks -Dexec.args="0"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerAcks -Dexec.args="1"
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerAcks -Dexec.args="all"
```

`acks=0` is fastest and `acks=all` slowest, but on a healthy 3-broker cluster the gap is
small — and only `acks=all` guarantees no loss if a broker fails mid-write.

> **If a sharp student asks:** with `acks=all`, "all" means all *in-sync* replicas. If the
> ISR shrinks to just the leader, "all" is one broker — which is why real durability also
> needs `min.insync.replicas=2` on the topic. We make that hands-on in Module 7.

---

## Exercise 5 — The Idempotent Producer

> **What this shows:** with retries, a lost ack causes the producer to resend a record the
> broker already wrote — a **duplicate**. `enable.idempotence=true` makes the broker
> de-duplicate the producer's retries (via a producer id + per-partition sequence number),
> giving exactly-once *delivery to the broker* while preserving order.

### 5.1 Turn it on

```java
// save as ProducerIdempotent.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerIdempotent {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);   // implies acks=all + safe retries

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < 5; i++) {
        producer.send(new ProducerRecord<>("lab02-orders", "user-1", "idempotent-" + i));
      }
      producer.flush();
    }
    System.out.println("sent 5 idempotent records for key user-1");
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerIdempotent
```

It runs like a normal producer — the guarantee is invisible until a retry happens. The
point is that enabling it is essentially free, so it's a sensible default for any producer
you care about.

### 5.2 Reason about the boundary

Answer these before moving on (discussion, not code):

1. If **your program** runs `ProducerIdempotent` twice, do you get duplicates in the
   topic? Why does idempotence not help here?
2. Idempotence de-dupes retries **to one partition**. Which stronger feature would you need
   to make a *consume → process → produce* step exactly-once across partitions?

> **If a sharp student asks:** what's the actual mechanism? The producer is assigned a
> Producer ID (PID); each record carries a monotonic sequence number per partition. The
> broker remembers the last sequence it accepted per (PID, partition) and silently drops a
> record whose sequence it has already seen — so a retried batch can't be written twice.

---

## Review Questions

1. `send()` returned without throwing but the record never reached the topic. Give two
   reasons this can happen and the one call/mechanism that would have surfaced the problem.
2. You need all events for a given `account_id` processed in order. What must you set on the
   producer, and what is the resulting guarantee's scope?
3. Raising `linger.ms` from 0 to 20 increased throughput but the average latency barely
   moved under load. Why?
4. Your team sets `acks=all` and believes data is safe on multiple brokers, but a broker
   failure still lost acknowledged records. What topic-level setting was probably missing?
5. Explain the duplicate scenario that `enable.idempotence=true` prevents, and name one
   duplicate scenario it does **not** prevent.
6. Why is `enable.idempotence=true` considered a reasonable default rather than a
   specialized option?

## What's Next

You can now produce robustly. Next is the other half of the client story:
**Module 6 (Consumer Internals)** and **Lab 03** — consumer groups and rebalancing, offset
management (auto vs. manual commit), seeking and replay, and building consumers that
survive rebalances.
