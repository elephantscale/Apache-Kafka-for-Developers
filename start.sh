#!/usr/bin/env bash
# start.sh — one command to get your lab environment ready each morning.
#
#   ./start.sh              # Day 1 / Day 2 of Intermediate: the core cluster
#   ./start.sh connect      # + Kafka Connect, Postgres, MinIO      (Lab 06)
#   ./start.sh flink        # + Flink                               (Lab 07)
#   ./start.sh all          # + monitoring (Prometheus, Grafana)    (Lab 08)
#
# It pulls the latest course updates, starts the containers you need, waits until
# the cluster is actually healthy, and prints the URLs. Safe to re-run at any time.

set -uo pipefail
cd "$(dirname "$0")"

G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; B=$'\033[1m'; N=$'\033[0m'
say()  { printf "\n${B}%s${N}\n" "$1"; }
ok()   { printf "  ${G}OK${N}    %s\n" "$1"; }
bad()  { printf "  ${R}!!${N}    %s\n" "$1"; }
info() { printf "  ${Y}··${N}    %s\n" "$1"; }

PROFILES=()
case "${1:-core}" in
  core|"")               ;;
  connect)               PROFILES=(--profile connect) ;;
  flink)                 PROFILES=(--profile connect --profile flink) ;;
  all|monitoring)        PROFILES=(--profile connect --profile flink --profile monitoring) ;;
  -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
  *) echo "usage: ./start.sh [core|connect|flink|all]" >&2; exit 2 ;;
esac

# ---------------------------------------------------------------- course updates
say "1. Getting the latest course updates"
if git pull --ff-only 2>/dev/null | tail -1 | grep -qi "up to date"; then
  ok "already up to date"
elif git pull --ff-only >/dev/null 2>&1; then
  ok "updated — lab guides and scripts are current"
else
  info "could not pull (offline, or local edits) — continuing with what you have"
fi

# ------------------------------------------------------------------- containers
say "2. Starting containers"
[ ${#PROFILES[@]} -gt 0 ] && info "including: ${1}"
if docker compose "${PROFILES[@]}" up -d >/dev/null 2>&1; then
  ok "docker compose up -d issued"
else
  bad "docker compose failed — is Docker running?"; exit 1
fi

# ---------------------------------------------------------------------- healthy
say "3. Waiting for the cluster"
for i in $(seq 1 60); do
  n=$(docker exec kafka-1 kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 2>/dev/null | grep -c "id:")
  [ "${n:-0}" = "3" ] && break
  sleep 2
done
if [ "${n:-0}" = "3" ]; then ok "3 brokers ready"
else bad "only ${n:-0} brokers responding — wait a moment and re-run ./start.sh"; fi

curl -sf http://localhost:8081/subjects >/dev/null 2>&1 \
  && ok "schema registry ready" || info "schema registry still starting"

if [ ${#PROFILES[@]} -gt 0 ]; then
  info "Kafka Connect installs its plugins on first start — this can take a few minutes"
  for i in $(seq 1 90); do
    curl -sf http://localhost:8083/connector-plugins >/dev/null 2>&1 && break
    sleep 2
  done
  p=$(curl -s http://localhost:8083/connector-plugins 2>/dev/null \
        | grep -o 'JdbcSourceConnector\|S3SinkConnector' | sort -u | wc -l | tr -d ' ')
  [ "$p" = "2" ] && ok "Connect ready with the JDBC + S3 plugins" \
                 || info "Connect still installing plugins (have $p of 2) — re-run ./start.sh later"
fi

# ------------------------------------------------------------------------- URLs
say "You're ready"
cat <<EOF
  Kafka          localhost:9092, 9093, 9094
  Kafka UI       http://localhost:8080
  Schema Registry http://localhost:8081
EOF
[ ${#PROFILES[@]} -gt 0 ] && cat <<EOF
  Kafka Connect  http://localhost:8083
  MinIO console  http://localhost:9001   (minioadmin / minioadmin)
EOF
case "${1:-core}" in
  flink|all|monitoring) echo "  Flink UI       http://localhost:8082" ;;
esac
case "${1:-core}" in
  all|monitoring) echo "  Grafana        http://localhost:3000" ;;
esac
cat <<'EOF'

  Your Java lab code lives in  labs/kafka-labs/src/main/java/com/elephantscale/kafka/
  Run a class with            cd labs/kafka-labs && ./run.sh <ClassName> [args]
EOF
