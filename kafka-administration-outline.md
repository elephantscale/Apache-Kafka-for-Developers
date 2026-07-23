# Apache Kafka Administration

**Format:** 4 days (32 hours) — deliverable as a 3-day core + optional 4th day.
**Level:** Intermediate to Advanced. Operations-focused, hands-on, lab-intensive.
**Kafka version:** Apache Kafka 4.x (KRaft mode — ZooKeeper-free), current as of 2026.

---

## Description

Running Apache Kafka in production is a discipline of its own. The developers who build on
Kafka rely on a platform that is installed correctly, configured for the workload, secured,
monitored, and kept healthy through upgrades, scaling, and failures. This course is for the
people who own that platform.

It is a hands-on **administration and operations** course that takes participants from cluster
architecture through the day-to-day and day-two tasks of operating Kafka: standing up a KRaft
cluster, administering topics and their configuration, securing the cluster with TLS and SASL,
monitoring broker and consumer health, tuning for throughput and durability, scaling brokers
and partitions without data loss, and performing zero-downtime rolling upgrades and disaster
recovery.

The course is current to **Apache Kafka 4**, which removed Apache ZooKeeper entirely in favor
of the built-in **KRaft** consensus protocol — so administrators learn today's controller-quorum
architecture rather than the legacy ZooKeeper operational model. Roughly half of every day is
hands-on: creating and reconfiguring topics, rotating certificates, diagnosing a cluster from
dashboards, reassigning partitions under load, and repairing clusters. All slides, labs, and
configuration are provided in a Git repository so the team can revisit and re-run the exercises
after the course.

The emphasis throughout is on **operating and administering the cluster** — the complement to a
developer course, aimed at the platform, SRE, and infrastructure engineers who keep Kafka
running.

## Audience

Platform engineers, SREs, infrastructure engineers, DevOps engineers, and administrators who
deploy, secure, monitor, and operate Kafka clusters. Also valuable for lead developers who own
their team's Kafka infrastructure. The cohort may be mixed in experience; pacing accounts for
that.

## Prerequisites

- Working familiarity with Kafka's core concepts — topics, partitions, offsets, producers,
  consumers, consumer groups (a Kafka fundamentals/developer course, or equivalent experience)
- Strong comfort with the **Linux command line**, shell, and editing configuration files
- Basic understanding of TLS/certificates and networking (helpful for the security module)
- Container / Kubernetes exposure is helpful for the Strimzi labs but not required
- Some exposure to monitoring tooling (Prometheus/Grafana or equivalent) is helpful

## Objectives

By the end of the course, participants will be able to:

- Explain Kafka 4's cluster architecture — brokers, the **KRaft** controller quorum, replication,
  and ISR — and how metadata is managed without ZooKeeper
- Install, configure, and validate a KRaft cluster (on plain hosts and on **Strimzi/Kubernetes**)
- Administer **topics** end to end: creation, partitions, replication, retention, compaction,
  per-topic configuration, quotas, and naming conventions
- Secure a cluster with **TLS encryption, SASL authentication, and ACL authorization**, and
  rotate credentials and certificates
- Monitor broker, producer, and consumer health; diagnose **consumer lag**, under-replicated
  partitions, and ISR shrinkage from dashboards and runbooks
- Tune broker, producer, and consumer settings for target throughput, latency, and durability
  SLAs, and benchmark a cluster
- Scale brokers and reassign partitions **without data loss**, and perform **zero-downtime
  rolling upgrades**
- Plan and execute **disaster recovery** and multi-cluster replication

---

## Course Outline

*Suggested durations sum to a 32-hour, 4-day course. Modules 1–6 form a 3-day core; Modules 7–8
(scaling/performance depth and DR) make the optional 4th day.*

---

# Module 1 — Kafka Architecture for Administrators

**Suggested duration:** 4 hours

**Learning outcomes:**

- Describe the anatomy of a Kafka 4 cluster: brokers, controllers, listeners, and clients
- Explain how KRaft manages metadata and replaces ZooKeeper operationally
- Relate replication, leaders, and ISR to availability and durability

- The Kafka cluster: brokers, partitions, replicas, leaders
- **KRaft** — the controller quorum, the metadata log, and how it replaces ZooKeeper
  - Process roles: `broker`, `controller`, and combined nodes
  - What changed operationally from the ZooKeeper era
- Replication, leaders, followers, and the **ISR** (in-sync replicas)
- Listeners, advertised listeners, and how clients discover the cluster
- Internal topics that administrators must know: `__consumer_offsets`, `__transaction_state`,
  cluster metadata

**Hands-on Lab:** Explore a running cluster's topology — brokers, controller quorum, partition
layouts, replica placement, and internal topics.

**Module review:** Trace how metadata, replication, and leadership determine cluster stability.

---

# Module 2 — Installation, Deployment & Configuration

**Suggested duration:** 4 hours

**Learning outcomes:**

- Stand up and validate a KRaft cluster from scratch
- Read and reason about the key broker configuration settings
- Compare bare-host, Docker, and Strimzi/Kubernetes deployment models

- Provisioning a KRaft cluster
  - Cluster ID, storage formatting, controller quorum voters
  - Combined vs. dedicated controller topologies
  - Directory layout, log directories, and data placement
- Broker configuration essentials
  - `num.io.threads`, `num.network.threads`, `num.replica.fetchers`
  - Heap sizing and GC tuning
  - Default replication and ISR settings (`default.replication.factor`, `min.insync.replicas`)
  - Log/segment and retention defaults
- Deployment models
  - Bare hosts / systemd
  - Docker Compose (the lab cluster)
  - **Strimzi on Kubernetes** — operators, KafkaNodePools, and declarative configuration
- Validating a new cluster: health checks, smoke tests, and first-boot verification

**Hands-on Lab:** Bring up a multi-broker KRaft cluster, apply a base configuration, and verify
health end to end.

**Module review:** Connect configuration choices to durability, throughput, and operability.

---

# Module 3 — Topic Administration

**Suggested duration:** 4 hours

**Learning outcomes:**

- Administer topics across their full lifecycle from the command line and programmatically
- Choose partition counts, replication factors, and retention/compaction policy for a workload
- Apply per-topic configuration, quotas, and governance conventions

- Topic lifecycle: create, describe, alter, delete
  - Partition count strategy and its effect on parallelism and ordering
  - Replication factor and rack awareness
  - Increasing partitions safely — and why you can't decrease them
- Retention and cleanup policy
  - Time- and size-based retention
  - **Log compaction** for changelog/state topics; `delete` vs. `compact`
  - Segment settings and their operational impact
- Per-topic configuration overrides (retention, compaction, max message size, etc.)
- **Quotas**: producer/consumer byte-rate and request quotas to protect the cluster
- Naming conventions, topic governance, and self-service topic management at scale
- Administering topics with the Admin API and infrastructure-as-code approaches

**Hands-on Lab:** Create and reconfigure topics — partitions, replication, retention, and
compaction — and apply quotas; observe the effects.

**Module review:** Map topic settings to workload requirements and cost.

---

# Module 4 — Security & Access Control

**Suggested duration:** 4 hours

**Learning outcomes:**

- Encrypt traffic and authenticate clients and brokers
- Authorize access with ACLs and administer them
- Rotate certificates and credentials without downtime

- Encryption in transit with **TLS**: keystores, truststores, broker and inter-broker TLS
- Authentication with **SASL** (SCRAM, and options like mTLS / OAuth)
- Authorization with **ACLs**: principals, operations, resource patterns, and administration
- Certificate and credential **rotation** procedures (avoiding client disruption)
- Audit logging: tracking producer/consumer access per topic for compliance
- Securing internal listeners and the controller quorum
- Security on **Strimzi**: listener types, `KafkaUser`, and operator-managed certificates

**Hands-on Lab:** Enable TLS and SASL on the cluster, define ACLs for a restricted user, and
verify authorized vs. denied access.

**Module review:** Build a defense-in-depth posture for an enterprise Kafka platform.

---

# Module 5 — Operations & Observability

**Suggested duration:** 4 hours

**Learning outcomes:**

- Select the metrics that matter for broker, producer, and consumer health
- Diagnose lag, imbalance, and replication problems from dashboards
- Apply operational runbooks to common support scenarios

- Key metrics: **under-replicated partitions**, **consumer lag**, request latency, ISR shrink
  rate, disk usage, network saturation
- The monitoring stack: **Prometheus + Grafana**, JMX exporters, Kafka UI / Kafdrop
- Consumer group administration: describe groups, reset offsets, detect stuck consumers,
  diagnose lag spikes
- Broker and partition operations: preferred leader election, rebalancing leadership
- Incident triage: an operational runbook workflow and escalation points
- Log and disk management, cleanup strategies, and early-warning indicators

**Hands-on Lab:** Diagnose cluster health from Grafana dashboards — induce and detect consumer
lag and under-replicated partitions, then remediate.

**Module review:** Turn raw telemetry into operational decisions before incidents become
outages.

---

# Module 6 — Reliability, Replication & High Availability

**Suggested duration:** 4 hours

**Learning outcomes:**

- Configure durability guarantees and understand their trade-offs
- Reason about failure behavior: leader election, ISR shrinkage, and recovery
- Design a cluster for a target availability

- Replication factor, `min.insync.replicas`, and `acks` as a durability contract
- Leader election: `unclean.leader.election.enable` and the availability-vs-durability trade-off
- Controlled shutdown vs. kill; graceful broker restarts
- Failure behavior: what happens when a broker or the active controller fails
- Rack awareness and broker placement for fault domains
- Monitoring and responding to ISR shrinkage and under-replicated partitions
- Designing for a target availability and defining recovery expectations

**Hands-on Lab:** Kill a broker under load; observe leader election, ISR changes, and recovery,
and verify no acknowledged data was lost.

**Module review:** Connect durability and HA settings to business SLAs.

---

# Module 7 — Scaling & Performance Tuning *(optional day)*

**Suggested duration:** 4 hours

**Learning outcomes:**

- Estimate Kafka capacity for a real workload and benchmark a cluster
- Scale brokers and reassign partitions without data loss
- Tune broker, producer, and consumer settings to hit throughput and latency targets

- Capacity planning: throughput modeling, storage sizing, retention impact
- Benchmarking with `kafka-producer-perf-test` and `kafka-consumer-perf-test`; establishing
  baselines and identifying network- vs. disk- vs. CPU-bound bottlenecks
- Expanding a running cluster **without data loss**
  - Adding brokers safely
  - **Partition reassignment** (`kafka-reassign-partitions`) and throttling replication to
    protect live producers
  - Validating ISR completeness after expansion
  - Strimzi-managed scaling with node pools and controlled rebalance
- Performance tuning: broker thread and GC tuning, producer `acks`/`linger.ms`/`batch.size`,
  consumer fetch tuning; balancing durability against throughput and tail latency

**Hands-on Lab:** Benchmark the cluster, then add a broker and reassign partitions under load
with replication throttling — confirming zero data loss.

**Module review:** Right-size and tune a cluster against a concrete SLA.

---

# Module 8 — Maintenance, Upgrades & Disaster Recovery *(optional day)*

**Suggested duration:** 4 hours

**Learning outcomes:**

- Plan and execute a zero-downtime rolling upgrade
- Design multi-cluster replication for disaster recovery
- Establish backup, retention, and recovery procedures

- **Zero-downtime rolling upgrades**
  - Ensuring ISR completeness before each broker restart; monitoring under-replicated
    partitions during the roll
  - Strimzi rolling restarts: PodDisruptionBudgets, `maxUnavailable`, preferred replica election
  - Upgrade sequencing, `metadata.version` / feature levels, and rollback considerations
- Multi-cluster replication and **disaster recovery**
  - MirrorMaker 2 and cluster-to-cluster replication
  - Active/passive and active/active patterns; failover and failback
  - Blue/green cluster migrations for major changes
- Backup and recovery: what to back up, offset translation, and recovery testing
- Maintenance windows, change management, and runbooks

**Hands-on Lab:** Perform a rolling upgrade of the cluster with zero downtime, watching ISR and
under-replicated partitions throughout.

**Module review:** Build a maintenance and DR playbook the team can operate against.

---

## Lab Environment

- Self-contained lab environment; **Apache Kafka 4 (KRaft)** with the supporting operational
  stack: Schema Registry, Kafka Connect, **Prometheus/Grafana**, and Kafka UI / Kafdrop
- Labs run on a local **Docker Compose** cluster and/or **Strimzi on Kubernetes** for the
  deployment and scaling exercises
- All labs, configuration, and slides are provided in a Git repository for participants to keep
  and re-run after the course

## Duration Options

- **4-day course:** Modules 1–8 (adds the scaling/performance and maintenance/DR depth)
- **3-day core:** Modules 1–6 (architecture, deployment, topic administration, security,
  operations, reliability)
- Pairs naturally with a Kafka **developer** course as the two halves of a full Kafka
  enablement track — developers build on the platform, administrators run it.
