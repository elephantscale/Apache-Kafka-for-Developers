#!/usr/bin/env bash
#
# verify-setup.sh — automated readiness check for the
# "Apache Kafka for Developers" lab VM.
#
# Usage:
#   ./verify-setup.sh          # fast prerequisite checks (+ smoke-test the
#                              #   cluster if it happens to be already running)
#   ./verify-setup.sh --full   # also `docker compose up -d`, wait for healthy,
#                              #   run the end-to-end smoke test, then leave it up
#   ./verify-setup.sh --full --down   # as --full, then tear the cluster down
#
# Exit code 0 = ready, non-zero = one or more checks FAILED.
# Safe to re-run; makes no changes unless --full is given.

set -u

FULL=0; DOWN=0
for a in "$@"; do
  case "$a" in
    --full) FULL=1 ;;
    --down) DOWN=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $a"; exit 2 ;;
  esac
done

# --- pretty output ----------------------------------------------------------
if [ -t 1 ]; then G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; B=$'\e[1m'; N=$'\e[0m'; else G=; R=; Y=; B=; N=; fi
PASS=0; FAIL=0; WARN=0
ok()   { printf "  ${G}PASS${N}  %s\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  ${R}FAIL${N}  %s\n" "$1"; FAIL=$((FAIL+1)); }
warn() { printf "  ${Y}WARN${N}  %s\n" "$1"; WARN=$((WARN+1)); }
head() { printf "\n${B}%s${N}\n" "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }

# --- locate the repo (so `docker compose` finds docker-compose.yml) ---------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR"
while [ "$REPO" != "/" ] && [ ! -f "$REPO/docker-compose.yml" ]; do REPO="$(dirname "$REPO")"; done

printf "${B}Apache Kafka for Developers — VM readiness check${N}\n"
printf "host: %s   user: %s   date: %s\n" "$(hostname)" "$(id -un)" "$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)"

# --- 1. Operating system & resources ---------------------------------------
head "1. Operating system & resources"
if [ "$(uname -s)" = "Linux" ]; then ok "OS is Linux ($(. /etc/os-release 2>/dev/null; echo "${PRETTY_NAME:-unknown}"))"
else warn "OS is $(uname -s) — course targets Linux/macOS"; fi

mem_gb=$(awk '/MemTotal/ {printf "%.1f", $2/1024/1024}' /proc/meminfo 2>/dev/null)
if [ -n "$mem_gb" ]; then
  awk "BEGIN{exit !($mem_gb+0 >= 7.0)}" && ok "RAM ${mem_gb} GB (>= 8 GB recommended)" \
    || warn "RAM ${mem_gb} GB — 8 GB recommended; 3 brokers may be tight"
fi
disk_gb=$(df -Pk "$REPO" 2>/dev/null | awk 'NR==2{printf "%.1f", $4/1024/1024}')
[ -n "$disk_gb" ] && { awk "BEGIN{exit !($disk_gb+0 >= 5.0)}" \
  && ok "Free disk ${disk_gb} GB on repo volume" || warn "Free disk ${disk_gb} GB — images need a few GB"; }

# --- 2. Docker engine & Compose v2 -----------------------------------------
head "2. Docker engine & Compose v2"
if have docker; then ok "docker present — $(docker --version 2>/dev/null)"
else bad "docker not installed — see labs/SETUP.md 'Install Docker'"; fi

if docker compose version >/dev/null 2>&1; then
  cv=$(docker compose version --short 2>/dev/null || docker compose version 2>/dev/null | head -1)
  case "$cv" in v2*|2.*) ok "docker compose v2 plugin — $cv" ;; *) warn "docker compose present but not clearly v2 ($cv)" ;; esac
else bad "docker compose v2 plugin missing (do NOT use legacy 'docker-compose')"; fi

if have docker && docker info >/dev/null 2>&1; then ok "docker usable by '$(id -un)' without sudo"
elif have docker; then bad "docker needs sudo — run: sudo usermod -aG docker \$USER  (then re-login)"; fi

if have docker && docker info >/dev/null 2>&1; then
  if docker run --rm hello-world >/dev/null 2>&1; then ok "docker can pull & run images (hello-world)"
  else bad "docker run failed — check daemon status / network egress to registry"; fi
fi

# --- 3. Java build environment (the labs are Java) --------------------------
head "3. Java build environment"
if have java; then
  jv=$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')
  [ "${jv:-0}" -ge 17 ] 2>/dev/null && ok "java $(java -version 2>&1 | head -1 | tr -d '"')" \
    || bad "java present but < 17 (need JDK 17): $(java -version 2>&1 | head -1)"
else bad "java not installed — need JDK 17 (e.g. Temurin/OpenJDK 17)"; fi

if have mvn; then ok "maven present — $(mvn -v 2>/dev/null | head -1)"
else bad "maven not installed — need Apache Maven 3.9+ (mvn)"; fi

# --- 3b. Maven can actually fetch the Kafka client --------------------------
# The commonest classroom failure is not a missing JDK but a filtered network:
# Maven Central (and, for Lab 05, the Confluent repo) unreachable from the VM.
head "3b. Maven dependency resolution"
if [ "$FULL" = 1 ] && have mvn; then
  if mvn -q -B dependency:get -Dartifact=org.apache.kafka:kafka-clients:4.0.2 >/dev/null 2>&1; then
    ok "kafka-clients:4.0.2 resolves from Maven Central"
  else bad "cannot resolve kafka-clients from Maven Central — check proxy/egress (see VM-SPEC.md)"; fi

  if mvn -q -B dependency:get -Dartifact=io.confluent:kafka-avro-serializer:8.0.6 \
       -DremoteRepositories=confluent::::https://packages.confluent.io/maven/ >/dev/null 2>&1; then
    ok "kafka-avro-serializer:8.0.6 resolves from the Confluent repo (Lab 05)"
  else warn "Confluent repo unreachable — Lab 05 (Schema Registry) will fail to build"; fi
else
  warn "skipped dependency resolution — run './verify-setup.sh --full' to test Maven egress"
fi

# --- 4. Repository ----------------------------------------------------------
head "4. Course repository"
if [ -f "$REPO/docker-compose.yml" ]; then ok "docker-compose.yml found at $REPO"
else bad "docker-compose.yml not found — clone/checkout the course repo"; fi

# --- helper: is the core cluster running? ----------------------------------
cluster_up() { have docker && [ -n "$(docker ps --filter 'name=kafka-1' --filter 'status=running' -q 2>/dev/null)" ]; }

# --- 5. Bring up the cluster (only with --full) -----------------------------
if [ "$FULL" = 1 ] && [ -f "$REPO/docker-compose.yml" ] && docker info >/dev/null 2>&1; then
  head "5. Starting the core cluster (--full)"
  ( cd "$REPO" && docker compose up -d ) && ok "docker compose up -d issued" || bad "docker compose up -d failed"
  printf "  ...waiting up to 120s for kafka-1/2/3 to become healthy"
  for i in $(seq 1 24); do
    healthy=$(docker ps --filter 'name=kafka-' --filter 'health=healthy' -q 2>/dev/null | wc -l)
    [ "$healthy" -ge 3 ] && break
    printf "."; sleep 5
  done; printf "\n"
  [ "${healthy:-0}" -ge 3 ] && ok "3 brokers report healthy" || warn "only ${healthy:-0}/3 brokers healthy — give it another minute"
fi

# --- 6. Cluster smoke test (if running) ------------------------------------
head "6. Cluster smoke test"
if cluster_up; then
  # host reachability
  if have nc; then nc -z localhost 9092 2>/dev/null && ok "broker port localhost:9092 reachable" || bad "localhost:9092 not reachable"; fi

  # CLI inside the container
  if docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    ok "kafka-topics.sh --list works inside kafka-1"
  else bad "kafka-topics.sh failed inside kafka-1"; fi

  # broker count (the SETUP.md smoke test)
  n=$(docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c "id:")
  [ "${n:-0}" = "3" ] && ok "cluster reports 3 brokers" || bad "cluster reports ${n:-0} brokers (expected 3)"

  # end-to-end produce/consume round trip
  T="verify-$$"
  if docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$T" --partitions 1 --replication-factor 3 >/dev/null 2>&1; then
    echo "hello-kafka" | docker exec -i kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$T" >/dev/null 2>&1
    got=$(docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$T" --from-beginning --max-messages 1 --timeout-ms 8000 2>/dev/null | tr -d '\r')
    [ "$got" = "hello-kafka" ] && ok "produce→consume round-trip on a replicated topic" || bad "round-trip failed (got: '${got}')"
    docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$T" >/dev/null 2>&1
  else bad "could not create test topic '$T'"; fi

  # web services
  if have curl; then
    curl -fsS http://localhost:8081/subjects >/dev/null 2>&1 && ok "Schema Registry up on :8081" || warn "Schema Registry :8081 not responding yet"
    curl -fsS -o /dev/null http://localhost:8080 2>/dev/null && ok "Kafka UI up on :8080" || warn "Kafka UI :8080 not responding yet"
  fi
else
  warn "core cluster not running — run './verify-setup.sh --full' to start and test it end-to-end"
fi

# --- optional teardown ------------------------------------------------------
if [ "$DOWN" = 1 ] && [ -f "$REPO/docker-compose.yml" ]; then
  head "Teardown (--down)"; ( cd "$REPO" && docker compose down ) && ok "docker compose down" || warn "teardown reported an issue"
fi

# --- summary ----------------------------------------------------------------
head "Summary"
printf "  ${G}%d passed${N}, ${Y}%d warnings${N}, ${R}%d failed${N}\n" "$PASS" "$WARN" "$FAIL"
if [ "$FAIL" -eq 0 ]; then
  printf "  ${G}${B}VM looks ready for the Apache Kafka for Developers labs.${N}\n"; exit 0
else
  printf "  ${R}${B}Not ready — resolve the FAIL items above (see labs/SETUP.md).${N}\n"; exit 1
fi