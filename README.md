# Apache Kafka for Developers

A hands-on developer course on Apache Kafka 4 (KRaft — ZooKeeper-free), taking
participants from fundamentals through intermediate streaming-application development.

**Format:** Introduction (1 day) + Intermediate (3 days) — deliverable as a 4-day track or
as two standalone courses.

## Contents

- **[outline.md](outline.md)** — full course outline (audience, prerequisites, objectives,
  module-by-module breakdown).
- `slides/` — module decks, Markdown → PPTX via `slides/gen.sh` + `slides/slide-list.txt`.
  **Scaffolded** (title + agenda stubs for all 11 modules); content to fill.
- `labs/` — hands-on lab guides (8 stubs, one per hands-on module). **Scaffolded**; content
  to fill. Setup in `labs/SETUP.md`.
- `docker-compose.yml` — Apache Kafka 4 (KRaft) 3-broker cluster + **Schema Registry** (`:8081`),
  Kafka UI (`:8080`), and on-demand profiles: `connect` (Postgres/MinIO/Kafka Connect),
  `monitoring` (Prometheus/Grafana), `flink` (Flink SQL, UI `:8082`).

## Build

```bash
# lab cluster
docker compose up -d                          # core: 3 brokers + Schema Registry + Kafka UI
docker compose --profile connect up -d        # + Kafka Connect / Postgres / MinIO
docker compose --profile flink   up -d        # + Flink (SQL)
# slides
cd slides && ./gen.sh                          # Markdown -> PPTX (per slide-list.txt)
```

> Scaffold note: infra (`docker-compose.yml`, `connect/`, `monitoring/`, `flink/`, `gen.sh`)
> is adapted from the verified **Advanced-Kafka-Streaming** repo; much lab content can be
> adapted from there where topics overlap (internals, connectors, reliability).

## Focus

Programming and hands-on practice over cluster administration: producers and consumers,
delivery guarantees and exactly-once, Schema Registry, Kafka Connect, and stream processing
with Kafka Streams and Flink SQL. Current to Apache Kafka 4.

*Companion to the advanced course, [Advanced Kafka with Streaming Architecture](https://github.com/elephantscale/Advanced-Kafka-Streaming).*