#!/usr/bin/env bash
#
# Drives the caching-workload benchmark.
#
#   - starts an N-node Infinispan Server cluster in Docker (host networking)
#   - waits for it to come up
#   - runs the Hot Rod workload client
#   - optionally injects a scale-up (add a node) or failure (kill a node) event
#   - captures container memory snapshots and leaves per-run JSON in results/
#
# See README.md for the per-step example invocations.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR="$HERE/client/target/caching-bench.jar"

# --- defaults --------------------------------------------------------------
IMAGE="quay.io/infinispan/server"
TAG="16.2"
STEP=1
CACHE_ARGS=""              # client cache-shape flags derived from STEP (owners/near-cache/...)
NODES=3
DURATION=60
WARMUP=10
SCALE_AT=""               # seconds: add a node at this point (rebalance)
KILL_AT=""                # seconds: kill node 1 at this point (failure)
BASE_PORT=11222
LABEL=""                  # optional run label, used in the result file name
CLUSTER="caching-bench"
HEAP="-Xms1g -Xmx1g"
USERNAME="admin"
PASSWORD="secret"
WORKLOAD_ARGS="--distribution uniform --read-ratio 0.9 --keys 200000 --value-size 1024 --threads 8"
RESULTS="$HERE/results"

# --- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
   case "$1" in
      --image)      IMAGE="$2"; shift 2 ;;
      --tag)        TAG="$2"; shift 2 ;;
      --step)       STEP="$2"; shift 2 ;;
      --nodes)      NODES="$2"; shift 2 ;;
      --duration)   DURATION="$2"; shift 2 ;;
      --warmup)     WARMUP="$2"; shift 2 ;;
      --scale-at)   SCALE_AT="$2"; shift 2 ;;
       --kill-at)    KILL_AT="$2"; shift 2 ;;
       --label)      LABEL="$2"; shift 2 ;;
       --base-port)  BASE_PORT="$2"; shift 2 ;;
      --heap)       HEAP="$2"; shift 2 ;;
      --workload)   WORKLOAD_ARGS="$2"; shift 2 ;;
      -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *) echo "unknown arg: $1" >&2; exit 2 ;;
   esac
done

# step -> client cache-shape flags (the cache is created at runtime on the cluster)
case "$STEP" in
   0)    CACHE_ARGS="--owners 2" ;;
   1)    CACHE_ARGS="--owners 1" ;;
   2)    CACHE_ARGS="--owners 1 --near-cache invalidated --near-cache-size 100000" ;;
   3)    CACHE_ARGS="--owners 1 --near-cache invalidated --near-cache-size 100000 --bloom" ;;
   4)    CACHE_ARGS="--owners 1 --persistence" ;;
   *) echo "unknown step: $STEP (expected 0-4)" >&2; exit 2 ;;
esac

[[ -f "$CLIENT_JAR" ]]      || { echo "client jar not built: run 'cd client && mvn -q -DskipTests package'" >&2; exit 1; }
command -v docker >/dev/null  || { echo "docker is required" >&2; exit 1; }

mkdir -p "$RESULTS"
RUN_NAME="${LABEL:-step${STEP}}-$(date +%H%M%S)"
echo ">>> step=$STEP cache=[$CACHE_ARGS] nodes=$NODES duration=${DURATION}s run=$RUN_NAME"

# extra client args so the JSON result is self-describing (label + injected event)
EXTRA_CLIENT_ARGS=""
if [[ -n "$LABEL" ]];    then EXTRA_CLIENT_ARGS+=" --label $LABEL"; fi
if [[ -n "$SCALE_AT" ]]; then EXTRA_CLIENT_ARGS+=" --scale-at $SCALE_AT"; fi
if [[ -n "$KILL_AT" ]];  then EXTRA_CLIENT_ARGS+=" --kill-at $KILL_AT"; fi

# always tear down on exit (success, failure, or signal) so a failed run never
# leaks containers that would steal the ports from the next run
cleanup() {
   local i
   for ((i = NODES + 1; i >= 1; i--)); do
      docker stop -t 1 "ispn-${RUN_NAME}-$i" >/dev/null 2>&1 || true
   done
}
trap cleanup EXIT

start_node() {
   local i="$1" port=$(( BASE_PORT + i - 1 ))
   docker run -d --rm \
      --name "ispn-${RUN_NAME}-$i" \
      --network host \
      -e USER="$USERNAME" -e PASS="$PASSWORD" \
      -e JAVA_OPTS_MEMORY="$HEAP" \
      "$IMAGE:$TAG" \
      -g "$CLUSTER" \
      -n "node-$i" \
      -p "$port" >/dev/null
}

wait_port() {
   local host="$1" port="$2" tries="${3:-120}" attempt
   for ((attempt = 0; attempt < tries; attempt++)); do
      if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
         exec 3>&- 3<&-
         return 0
      fi
      sleep 1
   done
   return 1
}

# --- start the cluster -----------------------------------------------------
for ((i = 1; i <= NODES; i++)); do start_node "$i"; done

echo ">>> waiting for nodes to accept connections..."
for ((i = 1; i <= NODES; i++)); do
   wait_port 127.0.0.1 "$(( BASE_PORT + i - 1 ))" 120 \
      || { echo ">>> node $i did not come up; last log lines:" >&2; docker logs "ispn-${RUN_NAME}-$i" 2>&1 | tail -n 50 >&2; exit 1; }
done
sleep 5   # let the cluster settle / form the consistent hash

HOSTS=""
for ((i = 1; i <= NODES; i++)); do HOSTS+="127.0.0.1:$(( BASE_PORT + i - 1 )),"; done
HOSTS="${HOSTS%,}"

snap_mem() {
   local out="$1"
   # List only containers that are actually running for this run; docker stats fails
   # entirely if handed a single non-existent name, so never enumerate by index.
   local names
   names="$(docker ps --filter "name=ispn-${RUN_NAME}-" --format '{{.Names}}' | tr '\n' ' ')"
   [[ -n "$names" ]] || { echo "(no running containers)" > "$out"; return 0; }
   docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' \
      $names > "$out" 2>/dev/null || true
}
snap_mem "$RESULTS/${RUN_NAME}.mem-start.txt"

# --- inject a scale / failure event at the requested time ------------------
EVENT_PID=""
if [[ -n "$SCALE_AT" ]]; then
   ( sleep "$SCALE_AT"; echo ">>> [t=${SCALE_AT}s] scaling up: adding node $((NODES + 1))"; start_node "$((NODES + 1))" ) &
   EVENT_PID=$!
fi
if [[ -n "$KILL_AT" ]]; then
   ( sleep "$KILL_AT"; echo ">>> [t=${KILL_AT}s] killing node 1"; docker stop "ispn-${RUN_NAME}-1" ) &
   EVENT_PID=$!
fi

# --- run the workload ------------------------------------------------------
echo ">>> running workload (hosts=$HOSTS)"
# shellcheck disable=SC2086
java -jar "$CLIENT_JAR" \
   --hosts "$HOSTS" \
   --cache cache \
   --username "$USERNAME" --password "$PASSWORD" --sasl-mechanism DIGEST-SHA-256 \
   --warmup "$WARMUP" --duration "$DURATION" \
    --result-dir "$RESULTS" --run-name "$RUN_NAME" \
    --step "$STEP" --nodes "$NODES" \
     $CACHE_ARGS $WORKLOAD_ARGS $EXTRA_CLIENT_ARGS

[[ -n "$EVENT_PID" ]] && wait "$EVENT_PID" 2>/dev/null || true

snap_mem "$RESULTS/${RUN_NAME}.mem-end.txt"
echo ">>> memory snapshots -> $RESULTS/${RUN_NAME}.mem-{start,end}.txt"

# --- teardown --------------------------------------------------------------
echo ">>> stopping cluster"
for ((i = NODES + 1; i >= 1; i--)); do
   docker stop -t 1 "ispn-${RUN_NAME}-$i" >/dev/null 2>&1 || true
done
echo ">>> done. results in $RESULTS/$RUN_NAME.*"
