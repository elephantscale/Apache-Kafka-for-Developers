# Labs Setup

This is a **developer** course. The lab environment is a self-contained **Docker
Compose** cluster you run on your own machine (or a provided VM). There is no
Kubernetes and no cluster administration to do — you bring the cluster up once and
spend the rest of the course writing producers, consumers, and stream-processing
code against it.

## Cluster Model & Versions

- **Apache Kafka 4.x in KRaft mode** — ZooKeeper-free. There is no ZooKeeper in any lab.
- A local **Docker Compose** cluster of **3 combined broker+controller nodes**
  (`kafka-1`, `kafka-2`, `kafka-3`), plus **Schema Registry** and a web **Kafka UI**.
- Optional add-on stacks are started on demand via Compose *profiles*:
  `connect` (Kafka Connect + Postgres + MinIO), `flink` (Flink SQL), and
  `monitoring` (Prometheus + Grafana).
- The Kafka CLI tools (`kafka-*.sh`) run **inside** the broker containers via
  `docker exec kafka-1 …`, so you never install Kafka itself. Your own code runs on
  the host in Java, so you **do** need a JDK and Maven.

### How clients connect

| From | Bootstrap server |
|------|------------------|
| Inside a container (`docker exec kafka-1 …`) | `localhost:9092` |
| Your Java code on the host | `localhost:9092` (also `9093`, `9094` reach the other brokers) |

Any one of these bootstrap addresses is enough — the client discovers the rest of
the cluster from broker metadata.

## Minimum Requirements

- Linux (Ubuntu) or macOS with **Docker** and the **Docker Compose v2** plugin
- **JDK 17** and **Maven 3.9+** — the labs are Java
- 8+ GB RAM (12 GB recommended; the three brokers are heap-capped to fit a small VM)
- An IDE is optional but recommended — see *Using an IDE* below

> Provisioning VMs for a class? See **[`VM-SPEC.md`](VM-SPEC.md)** for the full VM
> specification (sizing, pre-installed software, and offline/filtered-network image prep).

## Install Docker (one-time, per machine)

If Docker and the Compose **v2** plugin are already installed
(`docker compose version` prints `v2.x`), skip to *Java build environment*.

> Do **not** use `apt install docker.io` — the distro package is often too old and
> may not include the `docker compose` **v2** plugin the labs require.

These steps are for **Ubuntu/Debian**; on RHEL / Amazon Linux / Fedora use the
`dnf`-based equivalent, or ask your instructor.

```bash
# 1. Remove any old/conflicting packages (safe if none are present)
for p in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
  sudo apt-get remove -y $p 2>/dev/null || true
done

# 2. Add Docker's official apt repository
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 3. Install the engine, CLI, and the Compose v2 plugin
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 4. Run docker without sudo (so the labs' bare `docker exec ...` work)
sudo usermod -aG docker $USER
newgrp docker   # applies the group in THIS shell; otherwise log out/in
```

Verify:

```bash
docker --version          # Docker version 27.x or newer
docker compose version    # Docker Compose version v2.x  (note: "compose", no hyphen)
docker run --rm hello-world
```

> **Debian note:** in step 2 replace both `…/linux/ubuntu…` URLs with
> `…/linux/debian…`.

## Java build environment

The labs are **Java**, using the Kafka Java client and Maven. Confirm your toolchain:

```bash
java -version     # JDK 17.x
mvn -version      # Maven 3.9+
```

### The Maven project

All the Java in this course lives in **one ready-made Maven project** that ships with this
repo. You do not create a project, write a `pom.xml`, or make any directories:

```
labs/kafka-labs/
├── pom.xml                                  # all dependencies, already declared
├── run.sh                                   # compile + run helper
└── src/main/java/com/elephantscale/kafka/   # put your lab classes here
```

The `pom.xml` already includes everything any lab needs: `org.apache.kafka:kafka-clients`,
the Confluent Avro serde for Lab 05 (`io.confluent:kafka-avro-serializer`, with the Confluent
repository), Avro, and an SLF4J binding.

Save a lab class as `src/main/java/com/elephantscale/kafka/<ClassName>.java` — the file name
must match the class name, and the first line is `package com.elephantscale.kafka;` — then:

```bash
cd labs/kafka-labs
./run.sh ProducerBasic        # no arguments
./run.sh Feed 100             # with arguments
./run.sh                      # lists the classes you've written so far
```

`run.sh` **compiles before it runs**, checks that you're on JDK 17, and tells you exactly how
to switch if you're not. The compile step matters: `mvn exec:java` on its own does *not*
compile, so editing a class and re-running it would silently execute the previous version.

> **Doing it the long way.** `run.sh` is a convenience, not a requirement. The equivalent is:
> ```bash
> mvn -q compile
> mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.ProducerBasic -Dexec.args="..."
> ```
> Note the two steps — running `exec:java` without `compile` is the single most common way to
> lose ten minutes in these labs.

> **Prefer reading Python?** [`APPENDIX-python-reference.md`](APPENDIX-python-reference.md)
> has every producer/consumer/transaction example in the `confluent-kafka` Python client.
> It is optional — the labs themselves are Java — but the Kafka concepts and configs are
> identical in both.

> The first build downloads dependencies from Maven Central and the Confluent repo. If you are
> on a restricted network, see [`VM-SPEC.md`](VM-SPEC.md) for pre-caching the Maven repository.
> Running `./run.sh` once before class warms the cache for every lab.

### Using an IDE

The labs are written so that **the command line is always enough**. But because this is an
ordinary Maven project, any Java IDE will open it directly — and in an IDE you don't need
`run.sh` at all, because the IDE compiles for you:

- **IntelliJ IDEA** (Community Edition is fine — no Ultimate features are used):
  *File → Open →* select `labs/kafka-labs/pom.xml` → **Open as Project**. IntelliJ imports the
  dependencies itself; you don't need to run `mvn` first.
  Run a class with the green ▶ gutter arrow next to `main`. To pass the arguments some labs
  use, edit the run configuration and set *Program arguments* — the IDE equivalent of
  `./run.sh SomeClass those args`.
- **VS Code** with the *Extension Pack for Java*, or **Eclipse** (*Import → Existing Maven
  Projects*), work the same way.

Two things an IDE genuinely helps with in this course: **stepping through a consumer poll loop
in the debugger** (Lab 03) and **breakpointing inside a transaction** to watch what a
`read_committed` reader can and cannot see (Lab 04).

> Set the project SDK to **JDK 17** if the IDE offers a choice. There is only one project to
> open — every lab's classes live side by side in it, so you can flick between Lab 02's
> producer and Lab 04's transactional pipeline without reimporting anything.

## Quick Start — One Command

Each morning, from the repo root:

```bash
./start.sh              # the core cluster (3 brokers, Schema Registry, Kafka UI)
./start.sh connect      # + Kafka Connect, Postgres, MinIO   (Lab 06)
./start.sh flink        # + Flink                            (Lab 07)
./start.sh all          # + Prometheus and Grafana           (Lab 08)
```

It fetches the latest course updates, starts what you need, waits until the brokers are
genuinely healthy, and prints the URLs. Safe to re-run whenever something looks wrong.

The sections below explain what it is doing, and how to run the pieces by hand.

## Bring Up the Core Cluster

Run from the repository root (or any folder inside it — `docker compose` searches
parent directories for `docker-compose.yml`; it will not find the file from outside
the repo).

```bash
docker compose up -d
docker compose ps          # wait until kafka-1/2/3 are "healthy"
```

This starts the three brokers, **Schema Registry** (`:8081`), and the **Kafka UI**
(open <http://localhost:8080>).

## Optional Profiles (started only when a lab needs them)

```bash
# Lab 06 — Kafka Connect (adds Kafka Connect, Postgres, MinIO)
docker compose --profile connect up -d

# Lab 07 — Stream processing with Flink SQL
docker compose --profile flink up -d

# Lab 08 — Reliability & monitoring (Prometheus + Grafana)
docker compose --profile monitoring up -d
```

## Verification

```bash
# Broker reachable from the host
nc -zv localhost 9092

# List topics from inside a broker container
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# Count reachable brokers (should print 3)
docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep -c "id:"
```

For a full end-to-end readiness check (Docker, JDK/Maven, and a produce/consume smoke
test), run `./labs/verify-setup.sh --full` from the repo root.

## Shutting Down

```bash
docker compose down            # stop the cluster, keep nothing running
docker compose down -v         # also remove volumes (fresh start next time)
```