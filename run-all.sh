#!/usr/bin/env bash
# Runs the full caching-workload benchmark matrix, sequentially.
#
# Each run.sh invocation writes its own results/<label>-<time>.json + mem snapshots.
# This script only sequences the runs, logs progress, and records a manifest of the
# resulting JSON files so the numbers can be aggregated later.
#
#   ./run-all.sh            # runs everything
#
set -uo pipefail   # deliberately NOT -e: one failed run must not stop the rest

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R="$HERE/run.sh"
LOGDIR="/tmp/opencode/caching-bench"
mkdir -p "$LOGDIR"
MANIFEST="$LOGDIR/manifest.txt"
: > "$MANIFEST"

# fixed workload shape so every step is directly comparable
WL="--distribution zipf --read-ratio 0.9 --keys 200000 --value-size 1024 --threads 8"
NODES=3
DURATION=30
WARMUP=10
REPS=3

log() { echo "[$(date +%H:%M:%S)] $*"; }

run() {  # run <label> <step> [extra run.sh args...]
   local label="$1" step="$2"; shift 2
   log "START $label (step=$step | $*)"
   # safety net: remove any leftover containers for this label from a prior crash
   docker ps -aq --filter "name=ispn-${label}-" | xargs -r docker rm -f >/dev/null 2>&1 || true
   if "$R" --step "$step" --nodes "$NODES" --duration "$DURATION" --warmup "$WARMUP" \
           --workload "$WL" --label "$label" "$@" >"$LOGDIR/$label.log" 2>&1; then
      log "DONE  $label"
      echo "$label" >> "$MANIFEST"
   else
      log "FAIL  $label (see $LOGDIR/$label.log)"
      echo "FAIL $label" >> "$MANIFEST"
   fi
   sleep 3
}

log "=== steady-state matrix (nodes=$NODES duration=${DURATION}s reps=$REPS) ==="
for s in 0 1 2 3 4; do
   for r in $(seq 1 "$REPS"); do
      run "s${s}-r${r}" "$s"
   done
done

log "=== event tests (rebalance + recovery) ==="
# scale-up 3->4 nodes: single owner (s1) should rebalance less than two owners (s0)
run scale-s0 0 --duration 70 --scale-at 40
run scale-s1 1 --duration 70 --scale-at 40
# node kill: data loss with no store (s1) vs recovery via store (s4)
run kill-s1  1 --duration 70 --kill-at 40
run kill-s4  4 --duration 70 --kill-at 40

log "=== all runs complete; manifest: $MANIFEST ==="
