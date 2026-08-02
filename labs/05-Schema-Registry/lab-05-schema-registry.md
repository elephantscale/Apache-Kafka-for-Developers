# Lab 5 — Avro & the Schema Registry

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 4 — Serialization & Schema Registry
- **Duration:** ~75 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Language:** Java (Kafka Java client + Confluent Avro serdes, Maven)

## Objectives

By the end of this lab you will be able to:

- Set up a Maven project for the Kafka Java client with Avro serdes
- Define an Avro schema and produce records with `KafkaAvroSerializer`
- Inspect registered schemas and versions via the Schema Registry REST API
- Consume Avro records with `KafkaAvroDeserializer`
- Evolve a schema under BACKWARD compatibility and prove old consumers still work
- See the registry reject an incompatible change

## Prerequisites

- The core cluster running (`docker compose up -d`) — the **Schema Registry is part of the core
  stack** at `http://localhost:8081`, no extra profile needed
- **JDK 17** and **Maven** on the host (see the Java setup note below)
- Labs 02–03 completed (you understand producers and consumers)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers + Schema Registry,
> no ZooKeeper, no Kubernetes). This is the **first Java lab** — your code is a Maven project
> that runs on the host and connects to `localhost:9092` (Kafka) and `localhost:8081` (registry).
> The Confluent Avro serdes come from Confluent's Maven repository
> (`https://packages.confluent.io/maven/`).
>
> **Filtered-network note (onsite):** the first Maven build downloads the Kafka client, Avro,
> and Confluent serde jars. On a restricted network these must be available — either pre-populate
> the local Maven cache (`~/.m2`) in the VM image, or ensure `repo.maven.apache.org` and
> `packages.confluent.io` are reachable. Verify before class.

### Java project setup

Create a project folder `lab05/` with this `pom.xml`:

```xml
<!-- lab05/pom.xml -->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.elephantscale.kafka</groupId>
  <artifactId>lab05</artifactId>
  <version>1.0</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>4.0.2</version>
    </dependency>
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-avro-serializer</artifactId>
      <version>8.0.6</version>
    </dependency>
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>1.12.0</version>
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

Put Java sources under `lab05/src/main/java/com/elephantscale/kafka/`.

### Create the lab topic

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab05-orders --partitions 3 --replication-factor 3
```

---

## Exercise 1 — Produce Avro Records

> **What this shows:** with the Avro serde configured, producing a record automatically
> **registers the schema** (first time) and prefixes each message with the schema **ID**. You
> write objects; the serde handles the wire format and the registry.

### 1.1 Define the schema (a GenericRecord approach)

We'll build records with Avro's `GenericRecord` so no code generation is needed. Define the
schema in code:

```java
// src/main/java/com/elephantscale/kafka/AvroProducerApp.java
package com.elephantscale.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class AvroProducerApp {
  static final String SCHEMA_V1 = """
    {
      "type": "record",
      "name": "Order",
      "namespace": "com.elephantscale.kafka",
      "fields": [
        {"name": "orderId", "type": "int"},
        {"name": "amount",  "type": "double"}
      ]
    }
    """;

  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    p.put("schema.registry.url", "http://localhost:8081");

    Schema schema = new Schema.Parser().parse(SCHEMA_V1);

    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < 5; i++) {
        GenericRecord order = new GenericData.Record(schema);
        order.put("orderId", i);
        order.put("amount", 10.0 * i);
        producer.send(new ProducerRecord<>("lab05-orders", "key-" + i, order));
        System.out.println("sent orderId=" + i);
      }
      producer.flush();
    }
    System.out.println("done");
  }
}
```

### 1.2 Run it

```bash
cd lab05
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.AvroProducerApp
```

Five records are produced, and the schema is registered on the first send.

> **If a sharp student asks:** where did the schema get registered? The `KafkaAvroSerializer`
> called the registry the first time it saw this schema for subject `lab05-orders-value`,
> received an ID, and now prefixes every message with `[magic byte][schema id][avro bytes]`.
> Subsequent sends reuse the cached ID — no registry call per message.

---

## Exercise 2 — Inspect the Registry (REST API)

> **What this shows:** the registry is a plain REST service. You can see the subjects it knows,
> the versions of each, and the exact schema behind any ID — the single source of truth for the
> topic's data shape.

### 2.1 List subjects and versions

```bash
# all subjects
curl -s http://localhost:8081/subjects | jq .

# versions of our value subject
curl -s http://localhost:8081/subjects/lab05-orders-value/versions | jq .

# the actual schema for version 1
curl -s http://localhost:8081/subjects/lab05-orders-value/versions/1 | jq .
```

You'll see `lab05-orders-value` with version `1` and the `Order` schema you defined.

### 2.2 Check the compatibility mode

```bash
# global default (usually BACKWARD)
curl -s http://localhost:8081/config | jq .
```

> **If a sharp student asks:** why the `-value` suffix? Subjects default to
> `<topic>-key` and `<topic>-value` (the `TopicNameStrategy`). Keys and values evolve
> independently, so they're separate subjects. Other strategies exist for advanced
> multi-schema-per-topic setups, but topic-name is the default.

---

## Exercise 3 — Consume Avro Records

> **What this shows:** the consumer side is symmetric. The `KafkaAvroDeserializer` reads the
> schema ID off each message, fetches (and caches) the schema, and hands your code a decoded
> record — no manual parsing.

### 3.1 The consumer

```java
// src/main/java/com/elephantscale/kafka/AvroConsumerApp.java
package com.elephantscale.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class AvroConsumerApp {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // group id from the command line, default "lab05-consumer" -- Exercise 4 reuses this
    // same program with a *different* group so it re-reads the topic from the beginning
    p.put(ConsumerConfig.GROUP_ID_CONFIG, args.length > 0 ? args[0] : "lab05-consumer");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    p.put("schema.registry.url", "http://localhost:8081");

    try (Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(List.of("lab05-orders"));
      // Joining the group costs the first few seconds, and rejoining an existing group
      // (which is what you do in Exercise 4) costs more -- so give this plenty of room.
      // Too short a deadline and you exit before the first record arrives, which looks
      // exactly like "nothing was produced".
      long deadline = System.currentTimeMillis() + 30000;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, GenericRecord> r : records) {
          GenericRecord v = r.value();
          // Each record is decoded with the schema it was WRITTEN with, so a v1 record
          // has no "region" field at all. Ask the schema before reading it --
          // v.get("region") on a v1 record throws AvroRuntimeException, it does not
          // return null. This is what lets one consumer handle both versions.
          Object region = v.getSchema().getField("region") == null ? null : v.get("region");
          System.out.printf("P%d@%d  orderId=%s amount=%s region=%s%n",
              r.partition(), r.offset(), v.get("orderId"), v.get("amount"),
              region == null ? "(absent)" : region);
        }
      }
    }
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.AvroConsumerApp
```

You'll see the five decoded orders. Keep this consumer code — Exercise 4 reuses it against
evolved data.

---

## Exercise 4 — Evolve the Schema (BACKWARD)

> **What this shows:** the core evolution rule. Adding a field **with a default** is a BACKWARD-
> compatible change: a consumer built for the new schema can still read old records (they get the
> default), and the registry accepts the new version. This is how systems change without a
> coordinated outage.

### 4.1 Add a field with a default

Create a v2 producer that adds `region` with a default:

```java
// src/main/java/com/elephantscale/kafka/AvroProducerV2App.java
package com.elephantscale.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class AvroProducerV2App {
  // identical to v1 except for the new "region" field -- note the default,
  // which is what makes this a BACKWARD-compatible change
  static final String SCHEMA_V2 = """
    {
      "type": "record",
      "name": "Order",
      "namespace": "com.elephantscale.kafka",
      "fields": [
        {"name": "orderId", "type": "int"},
        {"name": "amount",  "type": "double"},
        {"name": "region",  "type": "string", "default": "UNKNOWN"}
      ]
    }
    """;

  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    p.put("schema.registry.url", "http://localhost:8081");

    Schema schema = new Schema.Parser().parse(SCHEMA_V2);

    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(p)) {
      for (int i = 5; i < 10; i++) {          // ids 5-9, so v1 and v2 records are easy to tell apart
        GenericRecord order = new GenericData.Record(schema);
        order.put("orderId", i);
        order.put("amount", 10.0 * i);
        order.put("region", "EAST");
        producer.send(new ProducerRecord<>("lab05-orders", "key-" + i, order));
        System.out.println("sent v2 orderId=" + i);
      }
      producer.flush();
    }
    System.out.println("done");
  }
}
```

Produce a few v2 records:

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.AvroProducerV2App
```

### 4.2 Confirm the new version registered

```bash
curl -s http://localhost:8081/subjects/lab05-orders-value/versions | jq .   # now [1, 2]
```

### 4.3 Prove old and new data both read

Re-run the **Exercise 3 consumer**, unchanged, but under a **new group id**:

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.AvroConsumerApp \
  -Dexec.args="lab05-consumer-v2"
```

The new group has no committed offsets, so `auto.offset.reset=earliest` takes effect and it
reads the topic from offset 0. (Reusing the Exercise 3 group would show you only the *new*
v2 records — that group already consumed and committed the v1 ones, and `auto.offset.reset`
is ignored once a committed offset exists. Same program, same topic, different read
position: the group **is** the bookmark.)

It reads **all** records —
the v1 orders print `region=(absent)` and the v2 orders print `region=EAST`. One consumer,
two schema versions, no redeploy: old data still works.

> **Worth saying out loud:** each record is decoded with the schema it was *written* with,
> which the registry supplies by id. That's why a v1 record has no `region` at all here
> rather than the `"UNKNOWN"` default. The default matters when a reader *asks* for v2 —
> then Avro fills in `UNKNOWN` for records that predate the field. That's exactly the
> guarantee BACKWARD compatibility gives you.

> **If a sharp student asks:** why doesn't the v1 record break the v2-aware consumer? Avro
> resolves the reader schema against the writer schema: the reader wants `region`, the v1 writer
> didn't have it, so the reader supplies the **default** (`UNKNOWN`). No default → this would be
> a breaking change. That single rule is 90% of safe evolution.

---

## Exercise 5 — Watch an Incompatible Change Get Rejected

> **What this shows:** the registry is an enforcement point, not just storage. A change that
> violates the subject's compatibility rule is rejected **at registration** — caught in your
> build/CI, not in production.

### 5.1 Test a breaking change (add a required field, no default)

You can test compatibility without producing, via the REST API:

```bash
curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.elephantscale.kafka\",\"fields\":[{\"name\":\"orderId\",\"type\":\"int\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"priority\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/lab05-orders-value/versions/latest | jq .
```

The response is `{"is_compatible": false}` — adding a **required** field (`priority`, no
default) breaks BACKWARD compatibility, because an old record has no value for it.

### 5.2 Fix it

Add `"default": "NORMAL"` to `priority` and re-run — now `{"is_compatible": true}`. The default
makes it a safe, backward-compatible addition.

> **If a sharp student asks:** could I just set the mode to NONE and force it through? Yes, and
> that's exactly the footgun the registry protects against. NONE disables checks — a producer
> change can then silently break every downstream consumer. Compatibility modes exist so those
> breaks are caught before deploy.

---

## Review Questions

1. A message on the wire is `[magic byte][schema id][avro bytes]`. Why send the ID instead of
   the whole schema, and how does the consumer turn the ID back into a schema?
2. Your topic `claims` is keyed. Name the two subjects the registry creates and explain why they
   evolve separately.
3. Under BACKWARD compatibility, which of these are safe: (a) add a field with a default, (b) add
   a required field, (c) remove a field that had a default? Explain each.
4. Because Kafka retains the log, a topic holds both v1 and v2 records. A consumer built for v2
   reads a v1 record — what value does it see for the v2-only field, and why doesn't it crash?
5. Your rollout must upgrade **producers first**, and old consumers must keep working against the
   new data. Which compatibility mode fits, and why?
6. A teammate proposes setting the subject to NONE "to move faster." What can go wrong in
   production, and what does BACKWARD buy you instead?

## What's Next

You can enforce and evolve data contracts. Next you'll move data **in and out** of Kafka without
writing client code: **Module 9 (Kafka Connect)** and **Lab 06** — source and sink connectors,
configuration, error handling, and Dead Letter Queues (uses the `connect` compose profile).
