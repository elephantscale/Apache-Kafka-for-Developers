# Lab 1 — Kafka Fundamentals

- **Course:** Apache Kafka for Developers
- **Module:** Intro 4 — Hands-On Fundamentals
- **Duration:** ~45 minutes
- **Difficulty:** Beginner
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)
- **Language:** Primarily CLI (`kafka-*.sh`); one optional Java/Maven smoke test

## Objectives

By the end of this lab you will be able to:

- Start the three-broker lab cluster and confirm it is healthy
- Create a topic and read its partition / replica / ISR layout
- Produce and consume events from the command line, with and without keys
- Run a consumer group, add a second member, and read partition assignment and **lag**

## Prerequisites

- The lab environment set up per [`labs/SETUP.md`](../SETUP.md) (Docker + Compose v2; JDK 17 +
  Maven for the optional Java smoke test in 1.2)
- A terminal open at the repository root
- You will need **three terminals** for the consumer-group exercise — open them now

## Lab Environment

> This is a **developer** lab against the local **Docker Compose** cluster from
> `labs/SETUP.md`: three combined broker+controller nodes (`kafka-1`, `kafka-2`,
> `kafka-3`) running **Apache Kafka 4.x in KRaft mode** — no ZooKeeper, no Kubernetes,
> nothing to administer. The Kafka CLI tools (`kafka-*.sh`) run **inside** the broker
> containers, so you invoke them with `docker exec kafka-1 …`. This lab is almost entirely
> CLI; the one optional host-side check in 1.2 uses the Kafka **Java** client via Maven,
> the same toolchain the rest of the course's labs use. Every command targets
> `--bootstrap-server localhost:9092`.

---

## Exercise 1 — Start and Inspect the Cluster

> **What this shows:** Kafka is a *cluster* of cooperating brokers, not one server.
> Before producing a single event, you confirm all three brokers are up and that a
> client can reach them and discover the whole cluster from one bootstrap address.

### 1.1 Bring up the core stack

From the repository root:

```bash
docker compose up -d
docker compose ps
```

Wait until `kafka-1`, `kafka-2`, `kafka-3` all show **`healthy`** (re-run
`docker compose ps` if they're still `starting` — it can take 20–40 seconds).

### 1.2 Confirm the cluster from a client

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

This returns quietly (no user topics yet) — but the fact that it returns at all means
the client connected and pulled cluster metadata.

Now confirm all three brokers are visible **from the host** (not just from inside a container).
The quickest check is a CLI call:

```bash
docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 \
  | grep -c "id:"
```

That prints `3` — one line per reachable broker.

*(Optional, Java)* To confirm the **host's own Java toolchain** — the one you'll use for every
later lab — can reach the cluster, add this tiny program to the Maven project introduced in Lab 05
(it needs only the `kafka-clients` dependency) and run it:

```java
// src/main/java/com/elephantscale/kafka/ClusterCheck.java
package com.elephantscale.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import java.util.Map;

public class ClusterCheck {
  public static void main(String[] args) throws Exception {
    try (AdminClient admin = AdminClient.create(
             Map.of("bootstrap.servers", "localhost:9092"))) {
      var nodes = admin.describeCluster().nodes().get();
      System.out.println("brokers: " + nodes);
      System.out.println("count: " + nodes.size());
    }
  }
}
```

```bash
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ClusterCheck
```

You should see **three broker entries** (ids 1, 2, 3).

> **If a sharp student asks:** why does `localhost:9092` reach all three brokers? It
> doesn't directly — `9092` is `kafka-1`'s host listener. But the client uses it only
> to *bootstrap*: it fetches metadata listing every broker, then connects to each
> partition's leader directly. `9093` and `9094` reach `kafka-2` and `kafka-3` and would
> bootstrap the cluster equally well.

### 1.3 (Optional) Open the Kafka UI

Browse to <http://localhost:8080>. You'll see the cluster, its brokers, and (soon) your
topics — a visual companion to the CLI.

---

## Exercise 2 — Create and Describe a Topic

> **What this shows:** the two independent decisions you make for every topic —
> **partitions** (how much parallelism it allows) and **replication factor** (how many
> broker failures it survives) — and how those decisions appear physically as leaders,
> replicas, and the ISR.

### 2.1 Create the `orders` topic

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 3 --replication-factor 3
```

Expected: `Created topic orders.`

### 2.2 Describe it

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders
```

You'll see one line per partition, for example:

```
Topic: orders  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

- **Leader** — the broker that serves reads/writes for that partition
- **Replicas** — every broker holding a copy
- **Isr** — the *in-sync* replicas; all three listed means the partition is fully healthy

> **If a sharp student asks:** why is each partition's leader on a different broker?
> Kafka spreads leadership across brokers so the read/write load is balanced rather than
> piling onto one node. Your exact leader assignment may differ from the example — that's
> fine.

### 2.3 Try an impossible replication factor

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic too-safe --partitions 1 --replication-factor 4
```

This **fails** — you can't have 4 copies on 3 brokers. Read the error; it's a common
real-world mistake. (No cleanup needed; the topic was not created.)

---

## Exercise 3 — Produce and Consume

> **What this shows:** the log in motion. A producer *appends* events; a consumer *reads
> them forward*. Reading does not remove them — you can replay from the beginning any
> time.

### 3.1 Start a consumer (terminal A)

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning
```

Leave it running — it will print events as they arrive.

### 3.2 Produce events (terminal B)

```bash
docker exec -i kafka-1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

Type a few lines, pressing Enter after each — each line is one event:

```
order placed
order shipped
order delivered
```

Watch them appear in terminal A. Press `Ctrl-D` (or `Ctrl-C`) to stop the producer.

### 3.3 Prove replay

Stop the consumer (`Ctrl-C`), then start it again with `--from-beginning`. It reprints
**every** event — the log kept them; consuming didn't consume them away.

> **If a sharp student asks:** if I omit `--from-beginning`, why do I see nothing? Without
> it, a brand-new consumer with no committed offset starts at the *end* of the log and
> only shows events produced *after* it started. `--from-beginning` tells it to start at
> offset 0 instead.

---

## Exercise 4 — Keys and Partitioning

> **What this shows:** the message **key** decides the partition, and all events sharing a
> key land in the same partition — which is exactly how Kafka preserves per-entity
> ordering (all events for one user, one order, one device).

### 4.1 Consume with key and partition visible (terminal A)

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning \
  --property print.key=true --property print.partition=true --property key.separator=' | '
```

### 4.2 Produce keyed events (terminal B)

```bash
docker exec -i kafka-1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders \
  --property parse.key=true --property key.separator=:
```

Type keyed events (`key:value`):

```
user-1:placed
user-1:shipped
user-2:placed
user-1:delivered
user-2:shipped
```

In terminal A, note the **partition** column: every `user-1` event shares one partition,
every `user-2` event shares one partition (possibly a different one). Same key → same
partition, every time.

> **If a sharp student asks:** could `user-1` and `user-2` end up in the *same* partition?
> Yes — keys are hashed into 3 partitions, so two different keys can collide onto one
> partition. What's guaranteed is the reverse: one key never spreads across partitions.

---

## Exercise 5 — Consumer Groups, Assignment, and Lag

> **What this shows:** the read-side scaling model. A **group** shares a topic's
> partitions across its members; **lag** measures how far behind the group is; and adding
> a consumer triggers an automatic **rebalance** — up to the partition-count ceiling.

### 5.1 Generate a steady stream (terminal B)

Produce 300 events, one every ~0.2s, so there's something to lag behind:

```bash
docker exec -i kafka-1 bash -c 'for i in $(seq 1 300); do echo "user-$((i % 5)):event-$i"; sleep 0.2; done' \
  | docker exec -i kafka-1 kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic orders \
      --property parse.key=true --property key.separator=:
```

Leave this running. (It finishes in ~60s; re-run it if you need more.)

### 5.2 Start one consumer in a group (terminal A)

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --group billing
```

### 5.3 Inspect the group (terminal C)

```bash
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group billing
```

With one consumer, all three partitions have the **same** `CONSUMER-ID`:

```
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
billing  orders  0          ...             ...            ...   consumer-1-...
billing  orders  1          ...             ...            ...   consumer-1-...
billing  orders  2          ...             ...            ...   consumer-1-...
```

**LAG = LOG-END-OFFSET − CURRENT-OFFSET.** Re-run the describe command a few times while
the producer runs and watch LAG rise and fall.

### 5.4 Add a second consumer and watch the rebalance

Open **terminal D** and start a second member of the *same* group:

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --group billing
```

Re-run the describe command in terminal C. Now the three partitions are split across
**two** `CONSUMER-ID`s — Kafka rebalanced automatically, with no change to the producer.

### 5.5 Find the ceiling

Add a **third** and a **fourth** consumer (more terminals, same `--group billing`). At
three consumers each owns one partition; the **fourth stays idle** — there are only three
partitions to hand out. This is the parallelism ceiling from Module 2, live.

Stop the producer and all consumers (`Ctrl-C`) when done.

---

## Review Questions

1. You created `orders` with 3 partitions and RF 3. Which of those numbers limits how many
   consumers in one group can work in parallel, and which determines how many broker
   failures the topic survives?
2. In the `--describe` output, what does it mean if a partition's `Isr` lists fewer brokers
   than its `Replicas`?
3. Two events are produced with the key `user-1`. Are they guaranteed to be read in the
   order produced? Why or why not?
4. A consumer group shows `LAG = 5000` on one partition and `0` on the others. What is one
   plausible explanation?
5. You start a 4th consumer in a group reading a 3-partition topic. What happens to it, and
   why?
6. Without `--from-beginning`, a fresh consumer sees no old events. Where in the log does it
   start, and what one flag changes that?

## What's Next

You've driven Kafka from the command line. Next you'll do the same things **from code**:
**Module 5 (Producer Internals)** and **Lab 02**, where you write a Java producer and
control serialization, partitioning, batching, acks, and idempotence yourself.
