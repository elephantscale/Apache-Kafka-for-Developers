#!/usr/bin/env bash
# verify-labs.sh — run the actual course labs end to end and check the results.
#
# This is different from verify-setup.sh, which checks the ENVIRONMENT (Docker, JDK,
# Maven, cluster reachable). This script runs the LABS: it extracts every Java class
# out of the lab Markdown, compiles it, runs the exercises, and asserts the outcomes
# students are told to expect.
#
# Because it extracts from the Markdown, it verifies the lab TEXT — if a lab drifts
# from working code, this fails.
#
# Usage:
#   ./verify-labs.sh            # Labs 01-05 (core cluster only)   ~10 min
#   ./verify-labs.sh --all      # + Labs 06-08 (connect/flink/monitoring)  ~25 min
#   ./verify-labs.sh --keep     # leave the cluster running afterwards
#
# Exit 0 = every check passed.

set -uo pipefail

ALL=0; KEEP=0
for a in "$@"; do
  case "$a" in
    --all)  ALL=1 ;;
    --keep) KEEP=1 ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown option: $a" >&2; exit 2 ;;
  esac
done

G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; B=$'\033[1m'; N=$'\033[0m'
PASS=0; FAIL=0

# A copy of every result also goes here, so the run can be shared without
# copy/paste from the VM: commit the file and push, or screenshot the tail.
REPORT="$(cd "$(dirname "$0")/.." && pwd)/labs/verify-labs-report.txt"
: > "$REPORT"
log() { printf '%s\n' "$1" >> "$REPORT"; }

section() { printf "\n${B}%s${N}\n" "$1"; log ""; log "$1"; }
ok()   { printf "  ${G}PASS${N}  %s\n" "$1"; log "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { printf "  ${R}FAIL${N}  %s\n" "$1"; log "  FAIL  $1"; FAIL=$((FAIL+1)); }
info() { printf "  ${Y}··${N}    %s\n" "$1"; log "  ..    $1"; }

# assert <description> <expected> <actual>
assert_eq() {
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$2', got '$3')"; fi
}
# assert_contains <description> <needle> <haystack>
assert_contains() {
  case "$3" in *"$2"*) ok "$1" ;; *) bad "$1 (missing '$2')" ;; esac
}

# Container names. Override when this stack runs alongside another course's
# containers, or when you have already started the cluster yourself:
#   KAFKA_CONTAINER=kfd-kafka-1 KAFKA_CONTAINER2=kfd-kafka-2 SKIP_COMPOSE=1 ./verify-labs.sh
KBROKER="${KAFKA_CONTAINER:-kafka-1}"
KBROKER2="${KAFKA_CONTAINER2:-kafka-2}"

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="$REPO/labs/kafka-labs"
SRC="$PROJ/src/main/java/com/elephantscale/kafka"
K="docker exec $KBROKER"
KT="$K kafka-topics.sh --bootstrap-server localhost:9092"
KC="$K kafka-console-consumer.sh --bootstrap-server localhost:9092"

cd "$REPO"

# ---------------------------------------------------------------- prerequisites
section "0. Prerequisites"
command -v java >/dev/null || { bad "java not found"; exit 1; }
jmaj=$(java -version 2>&1 | grep -oE '[0-9]+(\.[0-9]+)+' | command head -1 | cut -d. -f1)
if [ "${jmaj:-0}" -ge 17 ] 2>/dev/null; then ok "JDK $jmaj"; else
  bad "JDK $jmaj — need 17. export JAVA_HOME to a 17 JDK and retry"; exit 1; fi
command -v mvn >/dev/null && ok "maven present" || { bad "maven not found"; exit 1; }

section "1. Cluster"
docker compose up -d >/dev/null 2>&1
for i in $(seq 1 60); do $KT --list >/dev/null 2>&1 && break; sleep 2; done
n=$($K kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c "id:")
assert_eq "3 brokers up" "3" "$n"
[ "$n" = "3" ] || { echo "cluster not healthy — aborting"; exit 1; }
for i in $(seq 1 30); do curl -sf http://localhost:8081/subjects >/dev/null 2>&1 && break; sleep 2; done
curl -sf http://localhost:8081/subjects >/dev/null 2>&1 && ok "schema registry up" || bad "schema registry not responding"

# ------------------------------------------------------- extract lab Java + build
section "2. Extract lab code from the Markdown and compile"
mkdir -p "$SRC"
find "$SRC" -name '*.java' -delete 2>/dev/null
python3 - "$REPO" "$SRC" <<'PY'
import re, sys, pathlib
repo, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
n = 0
for md in sorted((repo/"labs").glob("0*/lab-*.md")):
    for block in re.findall(r"```java\n(.*?)```", md.read_text(), re.S):
        m = re.search(r"public class (\w+)", block)
        if m and "static void main" in block:
            (out/f"{m.group(1)}.java").write_text(block); n += 1
PY
count=$(ls "$SRC"/*.java 2>/dev/null | wc -l | tr -d ' ')
info "extracted $count classes from the lab guides"
if (cd "$PROJ" && mvn -q -B compile >/tmp/vl-compile.log 2>&1); then
  ok "all $count lab classes compile"
else
  bad "compile failed — see /tmp/vl-compile.log"; tail -15 /tmp/vl-compile.log; exit 1
fi

run() { (cd "$PROJ" && ./run.sh "$@" 2>&1); }
mktopic() { $KT --create --topic "$1" --partitions "${2:-3}" --replication-factor 3 >/dev/null 2>&1; }
droptopic() { $KT --delete --topic "$1" >/dev/null 2>&1; sleep 2; }

# ------------------------------------------------------------------------ Lab 01
section "3. Lab 01 — Fundamentals"
droptopic orders; mktopic orders
out=$($KT --describe --topic orders 2>/dev/null)
assert_contains "orders topic created with 3 partitions" "PartitionCount: 3" "$out"
out=$($KT --create --topic too-safe --partitions 1 --replication-factor 4 2>&1)
assert_contains "RF=4 correctly rejected" "InvalidReplicationFactorException" "$out"
# note the -i: without it docker exec gives the console producer no stdin, so the
# piped lines are silently discarded and nothing is produced
printf 'order placed\norder shipped\n' | docker exec -i "$KBROKER" kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders >/dev/null 2>&1
got=$($KC --topic orders --from-beginning --timeout-ms 8000 2>/dev/null | wc -l | tr -d ' ')
assert_eq "produce/consume round-trip" "2" "$got"
out=$(run ClusterCheck)
assert_contains "ClusterCheck sees 3 brokers" "count: 3" "$out"

# ------------------------------------------------------------------------ Lab 02
section "4. Lab 02 — Producers"
droptopic lab02-orders; mktopic lab02-orders
out=$(run ProducerBasic)
assert_eq "ProducerBasic delivered 10 records" "10" "$(printf '%s' "$out" | grep -c '^ok ')"
out=$(run ProducerKeys)
same=$(printf '%s' "$out" | grep -cE '^\S+ -> partition\(s\) \[[0-9]+\]$')
assert_eq "each key maps to exactly one partition" "3" "$same"
out=$(run ProducerAcks all);        assert_contains "ProducerAcks all"   "acks=all" "$out"
out=$(run ProducerIdempotent);      assert_contains "ProducerIdempotent" "idempotent" "$out"
out=$(run ProducerThroughput 20 zstd); assert_contains "ProducerThroughput zstd" "rec/s" "$out"

# ------------------------------------------------------------------------ Lab 03
section "5. Lab 03 — Consumers"
droptopic lab03-events; mktopic lab03-events
run Feed 100 >/dev/null
out=$(run ConsumerReplay)
tally=$(printf '%s' "$out" | grep -oE 'replayed [0-9]+ records' | grep -oE '[0-9]+')
assert_eq "ConsumerReplay replays every partition" "100" "${tally:-0}"
out=$(run ConsumerManual)
assert_contains "ConsumerManual shows the crash-before-commit case" "CRASH" "$out"

# ------------------------------------------------------------------------ Lab 04
section "6. Lab 04 — Transactions"
for t in lab04-input lab04-output; do droptopic $t; mktopic $t; done
$K kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group lab04-eos >/dev/null 2>&1
run TxnAbort >/dev/null
c=$($KC --topic lab04-output --from-beginning --timeout-ms 8000 --isolation-level read_committed 2>/dev/null | wc -l | tr -d ' ')
u=$($KC --topic lab04-output --from-beginning --timeout-ms 8000 --isolation-level read_uncommitted 2>/dev/null | wc -l | tr -d ' ')
assert_eq "read_committed hides the aborted batch" "3" "$c"
assert_eq "read_uncommitted shows it" "6" "$u"

droptopic lab04-output; mktopic lab04-output
$K kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group lab04-eos >/dev/null 2>&1
run SeedInput 20 >/dev/null
run PipelineEos 5 >/dev/null 2>&1   # crashes on purpose
run PipelineEos   >/dev/null 2>&1 &
sleep 75; kill %1 2>/dev/null; pkill -f "exec.mainClass=com.elephantscale.kafka.PipelineEos" 2>/dev/null
$KC --topic lab04-output --from-beginning --timeout-ms 10000 --isolation-level read_committed 2>/dev/null | sort > /tmp/vl-eos.txt
tot=$(wc -l < /tmp/vl-eos.txt | tr -d ' '); dup=$(sort /tmp/vl-eos.txt | uniq -d | wc -l | tr -d ' ')
assert_eq "exactly-once across a crash: 20 outputs" "20" "$tot"
assert_eq "exactly-once across a crash: 0 duplicates" "0" "$dup"

# ------------------------------------------------------------------------ Lab 05
section "7. Lab 05 — Schema Registry"
droptopic lab05-orders; mktopic lab05-orders
curl -s -X DELETE "http://localhost:8081/subjects/lab05-orders-value?permanent=true" >/dev/null 2>&1
curl -s -X DELETE "http://localhost:8081/subjects/lab05-orders-value" >/dev/null 2>&1
run AvroProducerApp   >/dev/null
run AvroProducerV2App >/dev/null
v=$(curl -s http://localhost:8081/subjects/lab05-orders-value/versions)
assert_eq "schema evolved to two versions" "[1,2]" "$v"
out=$(run AvroConsumerApp "verify-$$")
assert_eq "consumer reads v1 and v2 records" "10" "$(printf '%s' "$out" | grep -c orderId)"
assert_contains "v1 records show the field as absent" "region=(absent)" "$out"
assert_contains "v2 records carry the new field"      "region=EAST"     "$out"

if [ "$ALL" = "0" ]; then
  section "Summary"
  printf "  %d passed, %d failed\n" "$PASS" "$FAIL"
  log "  $PASS passed, $FAIL failed"
  info "Labs 06-08 skipped — re-run with --all to include Connect, Flink and the capstone"
  if [ "$KEEP" = "1" ] || [ "${SKIP_COMPOSE:-0}" = "1" ]; then
    info "cluster left running"
  else
    docker compose down -v --remove-orphans >/dev/null 2>&1
    left=$(docker ps --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$left" = "0" ] && ok "cluster torn down (docker ps empty)" || bad "$left containers still running"
  fi
  [ "$FAIL" -eq 0 ] && exit 0 || exit 1
fi

# ------------------------------------------------------------------ Labs 06 - 08
section "8. Starting connect / flink / monitoring profiles"
[ "${SKIP_COMPOSE:-0}" = "1" ] || docker compose --profile connect --profile flink --profile monitoring up -d >/dev/null 2>&1
for i in $(seq 1 90); do curl -sf http://localhost:8083/ >/dev/null 2>&1 && break; sleep 2; done
curl -sf http://localhost:8083/ >/dev/null 2>&1 && ok "Connect REST up" || bad "Connect REST not responding"
plug=$(curl -s http://localhost:8083/connector-plugins 2>/dev/null | grep -oE 'JdbcSourceConnector|S3SinkConnector' | sort -u | wc -l | tr -d ' ')
assert_eq "JDBC + S3 plugins installed" "2" "$plug"

section "9. Lab 06 — Connect"
docker exec -i "${POSTGRES_CONTAINER:-postgres}" psql -U kafka_user -d orders_db >/dev/null 2>&1 <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY, order_id VARCHAR(32) NOT NULL, customer_id VARCHAR(32) NOT NULL,
  amount NUMERIC(10,2) NOT NULL, status VARCHAR(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now());
INSERT INTO orders (order_id, customer_id, amount, status)
VALUES ('V-1','c-1',42.00,'NEW'),('V-2','c-2',108.50,'NEW'),('V-3','c-3',19.99,'NEW');
SQL
curl -s -X DELETE http://localhost:8083/connectors/orders-source >/dev/null 2>&1
droptopic pg-orders
curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors --data '{
 "name":"orders-source","config":{
 "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector",
 "connection.url":"jdbc:postgresql://postgres:5432/orders_db",
 "connection.user":"kafka_user","connection.password":"kafka_pw",
 "table.whitelist":"orders","mode":"timestamp+incrementing",
 "timestamp.column.name":"updated_at","incrementing.column.name":"id",
 "topic.prefix":"pg-","poll.interval.ms":"2000","numeric.mapping":"best_fit",
 "value.converter":"org.apache.kafka.connect.json.JsonConverter",
 "value.converter.schemas.enable":"false"}}' >/dev/null
sleep 20
st=$(curl -s http://localhost:8083/connectors/orders-source/status | grep -o '"state":"RUNNING"' | head -1)
assert_contains "JDBC source RUNNING" "RUNNING" "${st:-none}"
row=$($KC --topic pg-orders --from-beginning --timeout-ms 10000 2>/dev/null | head -1)
assert_contains "NUMERIC rendered as a number, not base64" '"amount":42.0' "$row"

section "10. Lab 07 — Flink SQL"
for t in lab07-claims lab07-contacts lab07-claims-per-region; do droptopic $t; mktopic $t; done
run ClaimFeeder >/dev/null 2>&1
cat > /tmp/vl-flink.sql <<'SQL'
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE TABLE claims (
  claim_id INT, person_id STRING, region STRING, amount DOUBLE, event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH ('connector'='kafka','topic'='lab07-claims',
  'properties.bootstrap.servers'='kafka-1:9092','properties.group.id'='verify-flink',
  'format'='json','scan.startup.mode'='earliest-offset');
SELECT COUNT(*) AS total FROM claims;
SQL
docker cp /tmp/vl-flink.sql "${FLINK_CONTAINER:-flink-jobmanager}":/tmp/vl-flink.sql >/dev/null 2>&1
( docker exec "${FLINK_CONTAINER:-flink-jobmanager}" ./bin/sql-client.sh -f /tmp/vl-flink.sql > /tmp/vl-flink.out 2>&1 ) &
sleep 90; kill %1 2>/dev/null
top=$(grep -oE '\| \+U \|\s+[0-9]+' /tmp/vl-flink.out | grep -oE '[0-9]+$' | sort -n | tail -1)
if [ "${top:-0}" -ge 190 ] 2>/dev/null; then ok "Flink SQL counted the stream (reached ${top})"
else bad "Flink SQL count reached only ${top:-0} (expected ~200)"; fi
for J in $(docker exec "${FLINK_CONTAINER:-flink-jobmanager}" ./bin/flink list 2>/dev/null | grep -oE "[0-9a-f]{32}"); do
  docker exec "${FLINK_CONTAINER:-flink-jobmanager}" ./bin/flink cancel "$J" >/dev/null 2>&1; done

section "11. Lab 08 — Capstone"
for t in lab08-claims lab08-claims-per-region; do
  droptopic $t
  $KT --create --topic $t --partitions 3 --replication-factor 3 --config min.insync.replicas=2 >/dev/null 2>&1
done
out=$($KT --describe --topic lab08-claims 2>/dev/null)
assert_contains "durability contract on the topic" "min.insync.replicas=2" "$out"
run CapstoneIngest 200 >/tmp/vl-ingest.log 2>&1 &
sleep 8
docker kill "$KBROKER2" >/dev/null 2>&1
info "killed $KBROKER2 mid-ingest"
wait %1 2>/dev/null
assert_contains "ingest survived the broker kill" "acknowledged=200" "$(cat /tmp/vl-ingest.log)"
docker start "$KBROKER2" >/dev/null 2>&1; sleep 30
isr=$($KT --describe --topic lab08-claims 2>/dev/null | grep -c "Isr: [0-9],[0-9],[0-9]")
assert_eq "ISR restored on all 3 partitions" "3" "$isr"
tot=$($K kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic lab08-claims 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
assert_eq "no acknowledged records lost" "200" "$tot"

section "Summary"
printf "  %d passed, %d failed\n" "$PASS" "$FAIL"
log "  $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  printf "\n${B}Failures only (screenshot this):${N}\n"
  grep "^  FAIL" "$REPORT"
fi
printf "\n  full report written to %s\n" "${REPORT#$REPO/}"
printf "  to share it:  git add -A && git commit -m verify && git push\n"
if [ "$KEEP" = "1" ] || [ "${SKIP_COMPOSE:-0}" = "1" ]; then
  info "cluster left running"
else
  docker compose --profile connect --profile flink --profile monitoring down -v --remove-orphans >/dev/null 2>&1
  left=$(docker ps --format '{{.Names}}' | wc -l | tr -d ' ')
  [ "$left" = "0" ] && ok "cluster torn down (docker ps empty)" || bad "$left containers still running — check 'docker ps'"
fi
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
