# Intro 4 — Hands-On Fundamentals

Elephant Scale

---

## Agenda

- Start a Kafka cluster and inspect it
- Create topics with partitions and replication
- Produce and consume events from the command line
- Observe consumer-group partition assignment and lag

---

## From Concepts to Keyboard

The last three modules were the mental model. Now we make it real.

- Everything you learned — topics, partitions, offsets, groups, replication — you'll now
  **see and touch** from the command line
- The CLI tools are the same ones operators use; they're the fastest way to *understand* Kafka
  before writing a line of client code
- This module pairs with **Lab 01** — the slides give the map, the lab is the territory

**Goal:** by the end you can start a cluster, create a topic, move events through it,
and read consumer lag — confidently.

Notes: Keep laptops open. This module is meant to be run along with, not watched. Every command here reappears in Lab 01.

---

## The Lab Cluster

Our environment is the three-broker KRaft cluster from Module 3, as Docker containers.

```
  host machine
   ┌───────────────────────────────────────────┐
   │  docker compose                            │
   │   kafka-1   kafka-2   kafka-3   (brokers)  │
   │   :9092     :9093     :9094   ← host ports  │
   │   schema-registry :8081                    │
   │   kafka-ui        :8080                    │
   └───────────────────────────────────────────┘
```

- Bring it up: `docker compose up -d`
- The Kafka CLI tools live **inside** the broker containers — no JDK on your host
- Run them with `docker exec kafka-1 <tool>.sh …`

---

## The CLI Tools You'll Use

All ship inside the broker image, all take `--bootstrap-server localhost:9092`.

| Tool | What it does |
|---|---|
| `kafka-topics.sh` | create / list / describe / alter topics |
| `kafka-console-producer.sh` | type events on stdin → a topic |
| `kafka-console-consumer.sh` | read events from a topic → stdout |
| `kafka-consumer-groups.sh` | list groups, show assignment & **lag** |

```bash
# the shape of every command in this module:
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Notes: `docker exec -i` (interactive) is needed for the console producer so it can read your keystrokes. The consumer is fine without `-i`.

---

## Creating a Topic

You choose two things up front: **partitions** (parallelism) and **replication factor**
(durability).

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 3 --replication-factor 3
```

- `--partitions 3` → three ordered logs → up to 3 consumers in a group
- `--replication-factor 3` → three copies across our three brokers → survives a broker loss
- RF can't exceed the number of brokers (3 here)

Notes: This is the one decision students most often get wrong later — partitions for parallelism, replication for durability. They are independent knobs.

---

## Inspecting a Topic: Leaders, Replicas, ISR

`--describe` shows the physical layout — the Module 2 concepts made visible.

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders
```

```
Topic: orders  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

- **Leader** — the broker handling reads/writes for that partition
- **Replicas** — all brokers holding a copy
- **Isr** — the in-sync replicas (all 3 = fully healthy)

---

## Producing and Consuming

Two terminals. Producer on one, consumer on the other.

```bash
# terminal 1 — produce (type lines, each is an event)
docker exec -i kafka-1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

```bash
# terminal 2 — consume from the beginning
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning
```

- `--from-beginning` reads the whole log; omit it to read only new events
- Type in terminal 1, watch it appear in terminal 2 — the log in action

---

## Keys and Partitioning, Live

Add a key and you control **which partition** an event lands in.

```bash
docker exec -i kafka-1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders \
  --property parse.key=true --property key.separator=:
```

```
user-1:placed order A
user-1:placed order B      ← same key → same partition → ordered
user-2:placed order C
```

- Same key → same partition → guaranteed order for that entity
- `--property print.key=true` on the consumer shows the key alongside the value

Notes: This is the concept from Module 2 you can now prove: consume with `print.partition=true` and show that both `user-1` events share a partition.

---

## Consumer Groups and Lag

Give the consumer a `--group` and Kafka tracks its progress. **Lag** = how far behind
the group is.

```bash
# consume as a member of group "billing"
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --group billing

# inspect the group: assignment, offsets, and lag
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group billing
```

```
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
billing  orders  0          42              42              0    consumer-1-...
billing  orders  1          40              45              5    consumer-1-...
```

**LAG = LOG-END-OFFSET − CURRENT-OFFSET** — the single most useful health number in Kafka.

---

## Watch a Rebalance

Start a **second** consumer in the same group and watch partitions redistribute.

```
1 consumer, group "billing":        2 consumers, same group:
  P0 ─► consumer-1                    P0 ─► consumer-1
  P1 ─► consumer-1                    P1 ─► consumer-2   ← reassigned
  P2 ─► consumer-1                    P2 ─► consumer-2
```

- Kafka **rebalances** automatically — no code, no restart of the producer
- A 4th consumer would sit **idle** (only 3 partitions) — the ceiling from Module 2
- `--describe` again shows the new `CONSUMER-ID` per partition

Notes: This is the payoff slide — the whole consumer-group scaling story from Module 2, demonstrated live in 30 seconds.

---

## Lab 01 — What You'll Do

**Stop here and run the lab.** Everything you need is on the slides you just saw.

1. **Start & inspect** the cluster — confirm 3 healthy brokers
2. **Create** the `orders` topic (3 partitions, RF 3) and read its layout
3. **Produce & consume** from the CLI — with and without keys
4. **Scale a consumer group** and read **lag** and partition assignment

*→ `labs/01-Fundamentals/lab-01-fundamentals.md`*

Environment: 3-broker KRaft cluster via Docker Compose · **~45 minutes**

Notes: Circulate rather than presenting. The two places people get stuck are Docker not being up and a consumer that looks "broken" when it is simply idle at the end of the log.

---

## Debrief — What You Just Saw

Welcome back. Before we move on, let's connect the lab to the concepts:

- When you added a **4th consumer** to a 3-partition topic, what happened — and why?
- What did `--describe` tell you about **leader** vs. **replicas** vs. **ISR**?
- With keys, did the same key ever land on two different partitions?
- What was **lag** doing while a consumer was stopped?

> These four questions are the whole intro day in miniature. If all four have clean
> answers, you're ready for the intermediate material.

Notes: Ask, don't tell — let students answer. This is the checkpoint that reveals who is genuinely following before the pace increases tomorrow. If the room is quiet on the 4th-consumer question, re-draw the partition-ceiling diagram.

---

## Summary

- The lab cluster is **3 KRaft brokers** in Docker; CLI tools run **inside** via `docker exec`
- **Create** topics with `--partitions` (parallelism) and `--replication-factor` (durability)
- `--describe` reveals **leader / replicas / ISR** — Module 2 made visible
- **Keys** decide partitioning and preserve per-key order
- A **consumer group** shares partitions; **lag = end-offset − current-offset** is the key metric
- Adding consumers triggers an automatic **rebalance**, up to the partition ceiling
