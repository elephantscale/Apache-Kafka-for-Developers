# Lab VM Specification — Apache Kafka for Developers

Specification for the training VMs that run the course labs. Share this with the VM provider
(e.g. ProTech). The labs run a local multi-container Kafka cluster on each VM and build/run
**Java** client code against it.

> **Companion files:** [`SETUP.md`](SETUP.md) — how a student sets up and starts the cluster;
> [`verify-setup.sh`](verify-setup.sh) — an automated readiness check (`./verify-setup.sh` and
> `./verify-setup.sh --full`) that confirms a VM meets this spec end to end.

## Quantity

- **1 VM per student** + **1 for the instructor**. For a 15-student class: **16 VMs**.

## Base OS

- **Ubuntu 22.04 LTS or 24.04 LTS** (64-bit, x86-64). Other recent Linux is acceptable if it has
  Docker Engine + the Compose **v2** plugin; the setup instructions are written for Ubuntu/Debian.

## Sizing (per VM)

| Resource | Minimum | Recommended | Why |
|---|---|---|---|
| **vCPU** | 2 | 4 | 3 Kafka brokers + Connect/Flink containers + a JVM build |
| **RAM** | 8 GB | **12 GB** | 3 brokers (heap-capped) + Schema Registry + Connect/Flink/Postgres/MinIO when profiles are up |
| **Disk (free)** | 25 GB | 40 GB | Docker images (~6–8 GB), container data, Maven cache, logs |

> 12 GB is the practical target: the optional `connect`, `flink`, and `monitoring` profiles add
> several containers on top of the 3-broker core.

## Required Software (pre-installed)

- **Docker Engine** + **Docker Compose v2 plugin** (`docker compose`, not legacy `docker-compose`)
  - The student user must be in the `docker` group (run `docker` without sudo)
- **JDK 17** (e.g. Temurin/OpenJDK 17) — the labs are Java
- **Apache Maven 3.9+** (`mvn`)
- **git**, **curl**, **jq**, **netcat (nc)**
- *(Optional)* Python 3.9+ — only if the Python reference versions of early labs are used

## Network / Internet Access

The first run pulls container images and Java dependencies from the internet. Ensure the VMs can
reach: `download.docker.com`, `registry-1.docker.io` (Docker Hub), `ghcr.io`,
`repo.maven.apache.org` (Maven Central), and `packages.confluent.io` (Confluent).

> **SSA delivery note:** students connect to **ProTech-hosted VMs that sit outside SSA's network**,
> so SSA's filtering does **not** affect image/dependency pulls — live pulls at class time are
> fine. Pre-staging is therefore **optional** (a nice-to-have for a faster, more reliable class
> start), not required. If you do want a fully self-contained image, pre-pull the images below and
> pre-populate the Maven cache (`~/.m2`) by building the lab project once.

### Docker images to pre-pull

```
apache/kafka:4.0.0
confluentinc/cp-schema-registry:7.7.0
ghcr.io/kafbat/kafka-ui:latest
confluentinc/cp-kafka-connect:7.7.0      # + confluent-hub JDBC and S3 connector plugins
postgres:16
minio/minio:latest
minio/mc:latest
flink:1.20.1-scala_2.12-java17
prom/prometheus:latest
grafana/grafana:latest
danielqsj/kafka-exporter:latest
```

> The `cp-kafka-connect` container installs the JDBC + S3 connector plugins via `confluent-hub`
> on first start (needs internet). For a fully offline image, **bake those plugins in** (build a
> derived image with the plugins pre-installed) rather than installing at container start.

### Java dependencies to pre-cache (build once on the golden image)

Building the lab Maven project once populates `~/.m2` with, among others:

```
org.apache.kafka:kafka-clients:3.9.0
io.confluent:kafka-avro-serializer:7.7.0        # from packages.confluent.io
org.apache.avro:avro:1.11.3
org.slf4j:slf4j-simple:2.0.13
org.codehaus.mojo:exec-maven-plugin:3.1.0
```

## Ports (localhost only, inside each VM)

No inbound access between VMs is needed; everything is local to the VM. Host ports used by the
labs: **9092/9093/9094** (Kafka), **8080** (Kafka UI), **8081** (Schema Registry), **8083**
(Kafka Connect), **9000/9001** (MinIO), **8082** (Flink UI), **9090/3000/9308** (Prometheus/
Grafana/exporter). These must be free on the VM.

## Access & Display

- SSH or console login for the student; a desktop is not required (labs are terminal + browser)
- A browser on the VM (or host) is helpful for the web UIs (Kafka UI, MinIO console, Grafana)

## Acceptance check

On a finished VM, running `./labs/verify-setup.sh --full` from the repo should report **0
failures** — it verifies Docker/Compose, JDK/Maven presence, brings up the cluster, and runs an
end-to-end produce/consume smoke test.
