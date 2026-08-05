# Lab 3 — Consumer Groups & Offsets

- **Course:** Apache Kafka for Developers
- **Module:** Intermediate 2 — Consumer Internals
- **Duration:** ~60 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Language:** Java (Kafka Java client, Maven)

## Objectives

By the end of this lab you will be able to:

- Write a Java consumer with a proper poll loop and error handling
- See how auto-commit can skip records, and fix it with manual commit
- Choose commit ordering to get at-least-once behavior, and reason about duplicates
- Seek and replay a partition from an arbitrary offset
- Use a rebalance listener to commit offsets before losing partitions

## Prerequisites

- The core cluster running (`docker compose up -d`, three brokers healthy)
- **JDK 17** and **Maven** on the host (see [`labs/SETUP.md`](../SETUP.md))
- Lab 02 completed (you have a working Java producer and the Maven project set up)

## Lab Environment

> Developer lab against the local **Docker Compose** cluster (3 KRaft brokers, no ZooKeeper,
> no Kubernetes). Your Java code runs on the host and connects to `localhost:9092`; Kafka CLI
> tools run inside the brokers via `docker exec kafka-1 …`.
>
> These programs use the small **Maven project** introduced in Lab 02: the `kafka-clients`
> dependency on the classpath, sources under `src/main/java/com/elephantscale/kafka/`, and each
> class run with `./run.sh <ClassName>`. (Consumers here use only `kafka-clients`
> — no Avro serde needed.)

### Create the lab topic and a feeder

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab03-events --partitions 3 --replication-factor 3
```

We'll drive it with a small producer you can re-run whenever a topic needs data:

```java
// src/main/java/com/elephantscale/kafka/Feed.java  — usage: ./run.sh Feed 100
package com.elephantscale.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class Feed {
  public static void main(String[] args) {
    int n = args.length > 0 ? Integer.parseInt(args[0]) : 100;
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (Producer<String, String> producer = new KafkaProducer<>(p)) {
      for (int i = 0; i < n; i++) {
        producer.send(new ProducerRecord<>("lab03-events", "user-" + (i % 5),
            "{\"seq\": " + i + "}"));
      }
      producer.flush();
    }
    System.out.println("produced " + n + " events");
  }
}
```

```bash
./run.sh Feed 100
```

---

## Exercise 1 — A Proper Poll Loop

> **What this shows:** a consumer is a loop of `poll → process → (commit)`. `poll()` returns a
> **batch** of records — possibly empty when the timeout elapses with no data — which you iterate.
> Errors surface as thrown exceptions, not a per-record error field, so a normal loop just handles
> the (often empty) batch each cycle.

### 1.1 Write the consumer

```java
// src/main/java/com/elephantscale/kafka/ConsumerBasic.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerBasic {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab03-basic");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");   // new group, no commits: start at 0
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
    Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));   // Ctrl-C -> clean stop
    consumer.subscribe(List.of("lab03-events"));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {   // empty batch on timeout -> loop again
          System.out.printf("P%d @ %d  %s%n", r.partition(), r.offset(), r.value());
        }
      }
    } catch (WakeupException e) {
      // expected on Ctrl-C
    } finally {
      consumer.close();                    // graceful leave -> faster rebalance
    }
  }
}
```

### 1.2 Run it

```bash
./run.sh Feed 100   # ensure data
./run.sh ConsumerBasic
```

You'll see all 100 events, labeled by partition and offset. Stop with `Ctrl-C`.

### 1.3 Run it again

Start it a second time. With **no new data** it prints nothing new — the group's committed
offsets are at the end. That's auto-commit having saved your position (default
`enable.auto.commit=true`).

> **If a sharp student asks:** why `auto.offset.reset=earliest`? It only applies the **first**
> time a group runs (no committed offset yet). After that, the committed offset wins. Change the
> `group.id` to see it re-read from the beginning as a brand-new group.

---

## Exercise 2 — How Auto-Commit Can Lose Data

> **What this shows:** with auto-commit, the consumer's **position** advances as `poll()` hands you
> records, and that position is committed automatically on an interval — *not* when your work is
> done. If you crash after the position was committed but before you finished processing those
> records, they are **skipped** on restart — silent data loss.

### 2.1 Simulate slow processing + a crash

```java
// src/main/java/com/elephantscale/kafka/ConsumerAutocommitLoss.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerAutocommitLoss {
  public static void main(String[] args) throws Exception {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab03-autoloss");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);   // auto-commits every 1s
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);             // one record per poll (clearer demo)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
    consumer.subscribe(List.of("lab03-events"));

    int count = 0;
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, String> r : records) {
        // Pretend processing is slow. The position already advanced when poll() returned this
        // record, so auto-commit can commit it while we're still "processing".
        Thread.sleep(500);
        count++;
        System.out.println("processed " + r.value() + " (count=" + count + ")");
        if (count == 5) {
          System.out.println("CRASH before finishing the batch!");
          Runtime.getRuntime().halt(1);     // hard exit — no clean commit/close
        }
      }
    }
  }
}
```

### 2.2 Run, crash, restart

```bash
./run.sh Feed 20
./run.sh ConsumerAutocommitLoss   # processes ~5, then hard-exits
./run.sh ConsumerAutocommitLoss   # restart — note where it resumes
```

On restart it likely **skips ahead**, past records it never actually finished — because the
interval had already committed those offsets. That gap is lost data.

> **If a sharp student asks:** is auto-commit always unsafe? No — it's fine when losing a few
> records doesn't matter (metrics, logs). The problem is only when "committed" must mean
> "processed." For that, commit manually.

---

## Exercise 3 — Manual Commit for At-Least-Once

> **What this shows:** turn auto-commit off and commit **after** processing. Now a crash before
> commit causes **re-processing**, not skipping — at-least-once. The price is possible duplicates,
> so processing should be idempotent.

### 3.1 Process first, then commit

```java
// src/main/java/com/elephantscale/kafka/ConsumerManual.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerManual {
  public static void main(String[] args) throws Exception {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab03-manual");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);      // WE decide when to commit
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
    consumer.subscribe(List.of("lab03-events"));

    int count = 0;
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, String> r : records) {
        Thread.sleep(200);                        // 1. process
        count++;
        System.out.println("processed " + r.value() + " (count=" + count + ")");
        if (count == 5) {
          System.out.println("CRASH after processing 5 but BEFORE committing it");
          Runtime.getRuntime().halt(1);           // record 5 processed, NOT committed
        }
        consumer.commitSync();                     // 2. THEN commit
      }
    }
  }
}
```

### 3.2 Run, crash, restart

```bash
./run.sh Feed 20
./run.sh ConsumerManual   # processes 5, crashes
./run.sh ConsumerManual   # restart
```

On restart it resumes at the **last committed** record. Records 1–4 were committed, but record 5
was processed and the crash beat its commit — so it is **processed again**: that's at-least-once,
and exactly why processing must be idempotent.

> **If a sharp student asks:** `commitSync` vs `commitAsync`? `commitSync()` blocks until the
> broker confirms — safest, and it retries. `commitAsync()` is faster but best-effort (no retry);
> you'd pair it with a final `commitSync()` on shutdown. Committing per-record is simplest to
> reason about; committing once per batch is a throughput optimization.

---

## Exercise 4 — Seek and Replay

> **What this shows:** committed offsets are only the *default* start. You can position a consumer
> anywhere in the retained log and re-read — the basis of replay, backfill, and
> reprocess-after-bugfix.

### 4.1 Replay the topic from the beginning

```java
// src/main/java/com/elephantscale/kafka/ConsumerReplay.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class ConsumerReplay {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab03-replay");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Map<Integer, Integer> perPartition = new TreeMap<>();
    int n = 0;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      // ask the cluster which partitions this topic has, and replay ALL of them --
      // don't assume a given key landed on any particular partition
      List<TopicPartition> all = new ArrayList<>();
      for (PartitionInfo info : consumer.partitionsFor("lab03-events")) {
        all.add(new TopicPartition(info.topic(), info.partition()));
      }

      consumer.assign(all);              // manual assignment: no group, no rebalance
      consumer.seekToBeginning(all);     // rewind to offset 0, ignore any commits

      int emptyPolls = 0;
      while (emptyPolls < 3) {           // first poll can be empty while we connect
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) { emptyPolls++; continue; }
        emptyPolls = 0;
        for (ConsumerRecord<String, String> r : records) {
          n++;
          perPartition.merge(r.partition(), 1, Integer::sum);
          System.out.printf("replayed P%d @ %d  %s%n", r.partition(), r.offset(), r.value());
        }
      }
    }
    System.out.println("replayed " + n + " records; per partition: " + perPartition);
  }
}
```

```bash
./run.sh ConsumerReplay
```

It re-reads **every** partition from offset 0, regardless of what any group committed — so the
count matches everything `Feed` has produced so far.

> **Why replay all partitions, not just partition 0?** Because `Feed` keys its records
> (`user-0`…`user-4`), and Kafka hashes those keys onto partitions. With 3 partitions those five
> keys happen to land on only **two** of them — partition 0 stays empty. Hardcoding partition 0
> would replay nothing and look like a broken program. Keys decide placement; never assume a
> particular partition has data. `partitionsFor()` asks the cluster instead of guessing.

> **If a sharp student asks:** how would I replay "everything since 9 AM"? Use
> `consumer.offsetsForTimes(...)` to convert a timestamp to the first offset at/after it, then
> `seek()` there. Same mechanism, timestamp instead of a literal offset.

---

## Exercise 5 — Commit on Rebalance

> **What this shows:** when a rebalance revokes your partitions, anything processed-but-not-
> committed would be re-processed by whoever picks them up. A rebalance listener lets you commit
> final offsets in `onPartitionsRevoked`, right before the partitions leave.

### 5.1 Add a rebalance listener

```java
// src/main/java/com/elephantscale/kafka/ConsumerRebalance.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConsumerRebalance {
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab03-rebalance");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
    Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

    ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        System.out.println("REVOKE  " + ids(parts) + " — committing first");
        try {
          consumer.commitSync();                 // flush progress before losing partitions
        } catch (Exception e) {
          System.out.println("  commit on revoke failed: " + e);
        }
      }
      @Override public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        System.out.println("ASSIGN  " + ids(parts));
      }
    };
    consumer.subscribe(List.of("lab03-events"), listener);

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {
          System.out.printf("P%d @ %d  %s%n", r.partition(), r.offset(), r.value());
          consumer.commitSync();
        }
      }
    } catch (WakeupException e) {
      // shutdown
    } finally {
      consumer.close();
    }
  }

  static String ids(Collection<TopicPartition> parts) {
    return parts.stream().map(tp -> String.valueOf(tp.partition()))
                .collect(Collectors.joining(",", "[", "]"));
  }
}
```

### 5.2 Trigger a rebalance

```bash
./run.sh Feed 300
# terminal A:
./run.sh ConsumerRebalance
# terminal B (same group — start while A runs):
./run.sh ConsumerRebalance
```

Watch terminal A: when B joins, A prints **REVOKE** for the partitions it gives up (committing
first), then **ASSIGN** for what it keeps. Stop B and A gets them back — another rebalance.

Inspect the group from a third terminal:

```bash
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group lab03-rebalance
```

> **If a sharp student asks:** with the KIP-848 next-gen protocol, do I still need this? Yes —
> the protocol makes rebalances *incremental and cheaper*, but the same lifecycle hooks apply.
> Committing before you lose a partition is good practice regardless of protocol.

---

## Review Questions

1. `poll()` returned an empty batch. Does that mean the topic is empty? What should your loop do?
2. A team uses default auto-commit and reports that after every crash "a few events go
   missing." Explain the mechanism and the one-line config change that fixes it.
3. You switched to manual commit and now occasionally see an event processed twice. Is this a
   bug? What property must your processing have, and why?
4. Put these in order for at-least-once delivery: `commit`, `process`, `poll`. Then give the
   order that produces at-most-once.
5. You deployed a fix and need to reprocess yesterday's data for one partition. Which two API
   calls do you use, and how would you start "from 9 AM" instead of offset 0?
6. Why commit offsets inside `onPartitionsRevoked`? What goes wrong if you don't?

## What's Next

You can produce and consume with real delivery control. Next you'll close the loop for true
end-to-end correctness: **Module 7 (Delivery Semantics & Transactions)** and **Lab 04** —
at-most/at-least/exactly-once, transactional producers, the consume-process-produce pattern,
and `read_committed` consumers.
