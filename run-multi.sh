#!/usr/bin/env bash
#
# Multi-client near-cache benchmark: N parallel client processes, each caching a
# DISJOINT slice of the keyspace, against one shared cluster. This is the setup where
# the near-cache bloom filter (selective invalidation, ISPN-5545) pays for itself:
#
#   - without bloom, the server sends every invalidation to EVERY client that has a
#     near cache on the cache, so a write to client 0's slice is also pushed to the
#     other N-1 clients that never cached that key (wasted fan-out + processing)
#   - with bloom, each client keeps a bloom filter of the keys it actually caches and
#     the server only pushes an invalidation to clients whose filter says "maybe I have
#     this key", so fan-out drops to ~1 client per write
#
# Each client is a separate JVM (its own connection + near cache), exactly like N
# real applications. An init client creates the cache and prefills the whole keyspace
# so every worker starts against a warm remote cache.
#
#   ./run-multi.sh --clients 8 --bloom --label multi8-bloom
#
# Reading AGG ops/s (see README "How to read the numbers"): with frac=1 each client's near
# cache holds its whole slice, so reads are local and AGG ops/s ~= 2x cluster WRITE capacity.
# Use --near-cache-frac N (>1) to shrink the near caches below the slices; uniform reads then
# miss at rate ~1-1/N and actually exercise the server's read path:
#   ./run-multi.sh --clients 8 --bloom --near-cache-frac 4 --label multi8-bloom-fc4
#
set -uo pipefail   # deliberately NOT -e: one failed worker must not abort the run

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR="$HERE/client/target/caching-bench.jar"

# --- defaults --------------------------------------------------------------
IMAGE="quay.io/infinispan/server"
TAG="16.3"
CLIENTS=8
NODES=3
KEYS=200000
VALUE_SIZE=1024
READ_RATIO=0.5        # more writes => more invalidations => bigger bloom effect
DISTRIBUTION="uniform"
THREADS=4             # per-client concurrency; keep total client CPU from dominating
DURATION=30
WARMUP=10
BLOOM=""              # set to "--bloom" to enable the near-cache bloom filter
NEAR_CACHE_FRAC=1     # per-client near cache = slice/frac; frac>1 makes reads miss (go remote) at ~1-1/frac
PIN_CPUS=""             # --pin-cpus N: pin worker i to CPUs [i*N, (i+1)*N) via taskset
BASE_PORT=11222
CLUSTER="caching-bench"
HEAP="-Xms1g -Xmx1g"
USERNAME="admin"
PASSWORD="secret"
RESULTS="$HERE/results"
LABEL=""

# --- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
   case "$1" in
      --clients)    CLIENTS="$2"; shift 2 ;;
      --nodes)      NODES="$2"; shift 2 ;;
      --keys)       KEYS="$2"; shift 2 ;;
      --value-size) VALUE_SIZE="$2"; shift 2 ;;
      --read-ratio) READ_RATIO="$2"; shift 2 ;;
      --distribution) DISTRIBUTION="$2"; shift 2 ;;
      --threads)    THREADS="$2"; shift 2 ;;
      --duration)   DURATION="$2"; shift 2 ;;
      --warmup)     WARMUP="$2"; shift 2 ;;
      --bloom)      BLOOM="--bloom"; shift ;;
      --near-cache-frac) NEAR_CACHE_FRAC="$2"; shift 2 ;;
       --pin-cpus)     PIN_CPUS="$2"; shift 2 ;;
      --tag)        TAG="$2"; shift 2 ;;
      --base-port)  BASE_PORT="$2"; shift 2 ;;
      --label)      LABEL="$2"; shift 2 ;;
      -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *) echo "unknown arg: $1" >&2; exit 2 ;;
   esac
done

[[ "$NEAR_CACHE_FRAC" =~ ^[1-9][0-9]*$ ]] \
   || { echo "near-cache-frac must be a positive integer, got: $NEAR_CACHE_FRAC" >&2; exit 2; }
SLICE=$(( KEYS / CLIENTS ))
# Near cache per client = slice/frac. frac=1 (default) => each near cache holds its whole
# slice and reads are local; AGG ops/s then ~= 2x cluster write capacity, NOT read scaling.
NC_SIZE=$(( SLICE / NEAR_CACHE_FRAC )); [[ $NC_SIZE -lt 64 ]] && NC_SIZE=64
RUN_NAME="${LABEL:-multi${CLIENTS}-$(date +%H%M%S)}"
echo ">>> multi-client: clients=$CLIENTS slice=$SLICE nearCacheSize=$NC_SIZE (frac=$NEAR_CACHE_FRAC) bloom=${BLOOM:-no} readRatio=$READ_RATIO pin=${PIN_CPUS:-off} nodes=$NODES run=$RUN_NAME"

[[ -f "$CLIENT_JAR" ]]      || { echo "client jar not built: run 'cd client && mvn -q -DskipTests package'" >&2; exit 1; }
command -v docker >/dev/null  || { echo "docker is required" >&2; exit 1; }
mkdir -p "$RESULTS"

cleanup() {
   local i
   for ((i = NODES; i >= 1; i--)); do
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
      -g "$CLUSTER" -n "node-$i" -p "$port" >"$RESULTS/${RUN_NAME}-node$i.docker.log" 2>&1 || { echo ">>> node $i failed to start:" >&2; cat "$RESULTS/${RUN_NAME}-node$i.docker.log" >&2; return 1; }
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
for ((i = 1; i <= NODES; i++)); do start_node "$i" || { echo ">>> node $i failed to start, aborting run (see ${RUN_NAME}-node$i.docker.log)" >&2; exit 1; }; done
echo ">>> waiting for $NODES nodes..."
for ((i = 1; i <= NODES; i++)); do
   wait_port 127.0.0.1 "$(( BASE_PORT + i - 1 ))" 120 \
      || { echo ">>> node $i did not come up:" >&2; docker logs "ispn-${RUN_NAME}-$i" 2>&1 | tail -n 50 >&2; exit 1; }
done
sleep 5

HOSTS=""
for ((i = 1; i <= NODES; i++)); do HOSTS+="127.0.0.1:$(( BASE_PORT + i - 1 )),"; done
HOSTS="${HOSTS%,}"

COMMON="--hosts $HOSTS --cache cache --username $USERNAME --password $PASSWORD --sasl-mechanism DIGEST-SHA-256"
WORKLOAD="--distribution $DISTRIBUTION --read-ratio $READ_RATIO --keys $KEYS --value-size $VALUE_SIZE --threads $THREADS"
CLIENT_HEAP="-Xms128m -Xmx512m"   # keep 8 client JVMs + 3 servers from pressing memory

# --- 1. init: create the cache + prefill the full keyspace -----------------
echo ">>> init: creating cache + prefilling $KEYS keys"
java $CLIENT_HEAP -jar "$CLIENT_JAR" $COMMON --owners 1 --init-only $WORKLOAD \
   >"$RESULTS/${RUN_NAME}-init.log" 2>&1 \
   || { echo ">>> init failed; log:" >&2; tail -n 40 "$RESULTS/${RUN_NAME}-init.log" >&2; exit 1; }
tail -n 2 "$RESULTS/${RUN_NAME}-init.log"

# --- 2. launch N workers in parallel, each on its own slice ----------------
echo ">>> launching $CLIENTS workers (near-cache size=$NC_SIZE, bloom=${BLOOM:-no})"
PIDS=()
for ((i = 0; i < CLIENTS; i++)); do
   # optional CPU pinning: dedicated core(s) per client JVM so fan-out cost is not
   # confounded with scheduling contention across the client processes (see README)
   PIN=( ); [[ -n "$PIN_CPUS" ]] && PIN=(taskset -c $((i*PIN_CPUS))-$(((i+1)*PIN_CPUS-1)))
   # shellcheck disable=SC2086
   "${PIN[@]}" java $CLIENT_HEAP -jar "$CLIENT_JAR" $COMMON \
      --owners 1 --near-cache invalidated --near-cache-size "$NC_SIZE" $BLOOM \
      --no-create-cache --no-prefill \
      --client-index "$i" --client-count "$CLIENTS" \
      --warmup "$WARMUP" --duration "$DURATION" \
      --result-dir "$RESULTS" --run-name "${RUN_NAME}-c${i}" \
      --label "$RUN_NAME" $WORKLOAD \
      >"$RESULTS/${RUN_NAME}-c${i}.log" 2>&1 &
   PIDS+=($!)
done

FAIL=0
for p in "${PIDS[@]}"; do
   wait "$p" || FAIL=1
done
[[ $FAIL -eq 0 ]] || echo ">>> warning: one or more workers exited non-zero (see logs)" >&2

# --- 3. aggregate ----------------------------------------------------------
echo ">>> aggregate:"
python3 "$HERE/aggregate-multi.py" "$RUN_NAME" "$CLIENTS" "$RESULTS"

echo ">>> done. per-client jsons: $RESULTS/${RUN_NAME}-c*.json"
