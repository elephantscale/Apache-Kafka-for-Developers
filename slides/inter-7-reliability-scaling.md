# Intermediate 7 — Reliability, Scaling & Operations

Elephant Scale

---

## Agenda

- Replication factor, `min.insync.replicas`, and HA configuration
- Consumer lag as the primary health metric; monitoring basics
- Producer/consumer performance tuning
- Partition-count strategy and its effect on scaling and ordering
- Hands-on / capstone: an end-to-end pipeline — ingest → process → sink — with reliability and
  monitoring

---

## Operating What You Build

You won't administer the cluster — but as a developer you own the reliability and performance of
**your** producers, consumers, and topics. This module is the developer's slice of operations.

- How to make **your** data durable and highly available
- How to tell whether **your** pipeline is healthy (and it's almost always **lag**)
- How to **tune** producers and consumers for throughput and latency
- How **partition count** shapes both scaling and ordering

*(Deep cluster administration — provisioning, security, upgrades, capacity — is a separate
administration course. Here we stay on the developer's side of the line.)*

Notes:
Set the boundary explicitly. This keeps the module developer-relevant and avoids drifting
into admin territory that belongs in the ops course.

---

## Durability, Recapped as a Contract

Three settings together define "acknowledged means safe." You met each earlier; here they are as
one contract.

- **Replication factor (RF)** — number of copies of each partition (topic-level; RF 3 in our lab)
- **`acks=all`** — the producer waits for all **in-sync** replicas before considering a write done
- **`min.insync.replicas`** — the minimum in-sync replicas that must acknowledge, or the write
  **fails**

```
RF=3, acks=all, min.insync.replicas=2
  → a write succeeds only when ≥2 of the 3 replicas have it
  → survives losing 1 broker with no data loss and no false "durable"
```

**This is the durability contract for data you can't lose.**

---

## Why `min.insync.replicas` Matters

`acks=all` alone isn't enough — "all in-sync replicas" can shrink to just the leader.

```
healthy:  ISR = {1,2,3}   acks=all waits for 3   ✓ safe
degraded: ISR = {1}       acks=all waits for 1   ✗ "durable" = one copy!
```

- Without a floor, a shrunken ISR silently weakens your durability to a single copy
- `min.insync.replicas=2` makes the producer **fail fast** instead of writing unsafely
- The trade-off: with RF 3 and min-ISR 2, you can lose **one** broker and keep writing; lose two
  and writes stop (correctly — you'd rather stop than lose data)

Notes:
This is the single most important reliability setting developers get wrong. acks=all is
half the contract; min.insync.replicas is the other half.

---

## High Availability in Action

What actually happens when a broker fails, with RF 3:

```
   partition orders-0:  leader=broker1, followers={2,3}
   broker1 dies →
   controller elects a new leader from the ISR (broker2)
   producers/consumers reconnect to broker2 automatically
```

- An **in-sync follower** is promoted — no acknowledged data is lost
- Clients follow metadata to the new leader; a brief retry, then business as usual
- This is why RF ≥ 3 + `min.insync.replicas=2` is the standard production baseline

**Your job as a developer:** enable retries/idempotence so the client rides through the failover
transparently (which the idempotent producer already does).

---

## Consumer Lag: The One Metric

If you watch a single number for pipeline health, watch **consumer lag**.

```
LAG = LOG-END-OFFSET (latest produced) − CURRENT-OFFSET (committed by the group)
```

- Lag ≈ 0 and steady → the consumer keeps up
- Lag **rising** → the consumer can't keep up (or has stalled) → data is getting staler
- Lag is **per partition, per group** — one hot partition can lag while others are fine

```
P0 lag=0     P1 lag=0     P2 lag=50000   ← investigate P2 / its consumer
```

Notes:
This is the SSA-stated concern. Lag is the heartbeat of a streaming app — everything else
is secondary diagnosis once lag tells you something is wrong.

---

## Monitoring Basics

You detect problems from a few signals, via a few tools.

- **What to watch:** consumer lag, under-replicated partitions, request latency, ISR shrink,
  broker disk/network
- **Tools:**
  - `kafka-consumer-groups.sh --describe` — lag on demand from the CLI
  - **Kafka UI** (`:8080`) — topics, groups, lag at a glance
  - **Prometheus + Grafana** (`monitoring` profile) — dashboards, history, alerts
  - **kafka-exporter** — exposes lag and cluster metrics to Prometheus

```
docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group billing        # LAG column per partition
```

For anything ongoing, wire lag into a dashboard with an **alert threshold** — don't eyeball it.

---

## Is It the Consumer or the Producer?

Rising lag has two shapes — diagnose which:

- **Producer surged** — input rate jumped; consumer is fine but behind temporarily → scale
  consumers or let it drain
- **Consumer slowed/stalled** — processing got slower or a consumer died → fix processing, check
  for rebalance loops

**First lever for a slow consumer group:** add consumers, **up to the partition count**.

```
3 partitions, 1 consumer, lag climbing
  → add 2 consumers (now 3) → each owns 1 partition → 3× throughput → lag drains
  → a 4th consumer sits idle (partition ceiling)
```

Notes:
This is the practical runbook. "Producer lag" (their phrase) usually means producer-side
latency/backpressure — buffer full, linger, acks — diagnosed on the producer, not the group.

---

## Producer Performance Tuning (Recap + Levers)

From Module 5, as tuning levers for throughput vs. latency vs. durability:

- **Throughput:** raise `linger.ms` and `batch.size`; enable `compression.type=zstd/lz4`
- **Durability:** `acks=all` + `enable.idempotence=true` (+ `min.insync.replicas=2` on the topic)
- **Latency:** lower `linger.ms`; smaller batches
- **Backpressure:** `buffer.memory` and `max.block.ms` govern what happens when the broker can't
  keep up — the producer blocks or throws rather than losing data

**There's no single "fast" config — you trade among the three. Decide which matters for the
workload.**

---

## Consumer Performance Tuning

Levers on the read side:

- **Parallelism** — more consumers up to partition count (the big one)
- **`max.poll.records`** — how many records per poll; smaller batches = more frequent commits and
  polls
- **`max.poll.interval.ms`** — the deadline to call `poll()` again; if processing a batch exceeds
  it, the consumer is considered dead and the group rebalances (a classic lag/rebalance-loop cause)
- **`fetch.min.bytes` / `fetch.max.wait.ms`** — batch fetches for efficiency vs. latency

**Common failure:** slow per-record processing blows past `max.poll.interval.ms` → rebalance
storm → lag spikes. Fix by processing smaller batches or moving work off the poll thread.

Notes:
The max.poll.interval rebalance trap is one of the most common real-world Kafka bugs —
call it out explicitly. It ties their "issues" and "lag" concerns together.

---

## Partition-Count Strategy

Partition count is the one topic decision that's hard to change later — it sets **both** scaling
and ordering.

- **More partitions** → more consumer parallelism, more throughput
- **But:** ordering is only guaranteed **within** a partition — more partitions = less global order
- You can **add** partitions, but it **breaks key→partition mapping** for existing keys (and you
  can't remove them)

```
choose for the parallelism you'll need:
  too few  → capped throughput, hot partitions
  too many → overhead, tiny batches, more end-to-end latency, weaker ordering
```

**Rule of thumb:** size partitions to your target peak throughput / per-consumer capacity, with
headroom — and set it deliberately up front.

---

## Common Issues & How to Debug

A developer's quick triage list (their "issues" concern):

- **Rising consumer lag** → check group `--describe`; add consumers to partition count; look for a
  stalled/rebalancing consumer
- **Rebalance loops** → processing exceeds `max.poll.interval.ms`; shrink batches / offload work
- **Under-replicated partitions** → a broker is down or a follower is behind; durability at risk
- **Producer `TimeoutException` / buffer full** → broker can't keep up or `acks`/network issue;
  check `buffer.memory`, `max.block.ms`
- **Duplicates downstream** → at-least-once without idempotent processing (or missing
  `read_committed`)
- **Uneven partition load** → poor key choice (a hot key); rethink the partition key

Notes:
This slide directly answers "issues, performance, configurations." It's a mini-runbook —
consider printing it as a takeaway card.

---

## Capstone (Lab 08): Bring It All Together

An end-to-end pipeline using everything from the course:

```
  Java producer          Flink SQL              Kafka Connect
  (ingest, acks=all,  ─►  (process:      ─►     (sink to        ─► monitor lag
   idempotent)            windowed agg)          MinIO / topic)     in Grafana
       │                                                             │
       └───────── RF=3, min.insync.replicas=2, 3 partitions ────────┘
                  + kill a broker and watch it survive
```

- **Ingest** durably, **process** in real time, **sink** the results, **monitor** lag
- Prove reliability: kill a broker mid-run and confirm no data loss
- *→ `labs/08-Reliability-Capstone/lab-08-reliability-capstone.md`*

---

## Summary

- Durability is a **contract**: RF + `acks=all` + `min.insync.replicas` — the last one is the
  half developers forget
- RF ≥ 3 + min-ISR 2 survives one broker loss with no data loss; clients ride failover via
  retries/idempotence
- **Consumer lag** is the primary health metric; watch it via CLI, Kafka UI, or Prometheus/Grafana
- Tune producers and consumers by **trading** throughput/latency/durability; mind the
  `max.poll.interval.ms` rebalance trap
- **Partition count** sets scaling *and* ordering — choose it deliberately up front
- The **capstone** integrates ingest → process → sink with reliability and monitoring
