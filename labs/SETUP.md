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

### The per-lab Maven project

Each lab is a small, self-contained Maven project — the lab guide gives you the exact
`pom.xml` and the classes to create. The shape is always the same:

```
labNN/
├── pom.xml                                  # dependencies (kafka-clients, etc.)
└── src/main/java/com/elephantscale/kafka/   # your .java sources go here
```

Every lab's `pom.xml` includes at least `org.apache.kafka:kafka-clients`; the Schema
Registry lab (Lab 05) adds the Confluent Avro serde (`io.confluent:kafka-avro-serializer`)
and therefore the Confluent Maven repository (`https://packages.confluent.io/maven/`).

Compile and run a class like this (the pattern used throughout the labs):

```bash
cd labNN
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.elephantscale.kafka.SomeClass
# pass program args with -Dexec.args="..."
```

> The first `mvn` build downloads dependencies from Maven Central (and, for Lab 05,
> the Confluent repo). If you are on a restricted network, see
> [`VM-SPEC.md`](VM-SPEC.md) for pre-caching the Maven repository.

### Using an IDE

The labs are written so that **Maven on the command line is always enough** — every lab
gives you the exact `mvn` commands, and nothing depends on an IDE. But because each lab
is an ordinary Maven project, any Java IDE will open one directly:

- **IntelliJ IDEA** (Community Edition is fine — no Ultimate features are used):
  *File → Open →* select the lab's `pom.xml` → **Open as Project**. IntelliJ imports the
  dependencies itself; you don't need to run `mvn` first.
  Run a class with the green ▶ gutter arrow next to `main`. To pass the arguments some
  labs use, edit the run configuration and set *Program arguments* — the IDE equivalent of
  `-Dexec.args="..."`.
- **VS Code** with the *Extension Pack for Java*, or **Eclipse** (*Import → Existing Maven
  Projects*), work the same way.

Two things an IDE genuinely helps with in this course: **stepping through a consumer poll
loop in the debugger** (Lab 03) and **breakpointing inside a transaction** to watch what a
`read_committed` reader can and cannot see (Lab 04).

> Set the project SDK to **JDK 17** if the IDE offers a choice. If you keep several labs
> open at once, open each `labNN/pom.xml` as its own project (or add them as Maven modules)
> — they are independent projects, not one reactor build.

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