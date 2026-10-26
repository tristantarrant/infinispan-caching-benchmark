# Caching Workload Benchmark

Harness for the blog series *"Tuning Infinispan for Caching Workloads"*. It
measures, one improvement at a time, how each configuration change affects memory,
throughput, latency, rebalancing and hot-key skew.

The cluster runs on the image's **default** server config; each "step" just creates a
differently-shaped cache **at runtime** through the Hot Rod admin API. The cache shape
is a set of client flags:

| Step | Cache shape (client flags) | What it shows |
|------|----------------------------|---------------|
| 0 | `--owners 2` | baseline memory / throughput / rebalance |
| 1 | `--owners 1` | memory ~halved, less write & rebalance traffic |
| 2 | `--owners 1 --near-cache invalidated --near-cache-size N` | hot-key read latency / throughput, owner offload |
| 3 | `... --bloom` | selective invalidation (lower client churn under writes) |
| 4 | `--owners 1 --persistence` | recovery of lost entries on node failure |

> Near cache and the bloom filter are **client-side**; only `owners` and `persistence`
> change the server-side cache, so steps 1-3 reuse the same server config.

## Layout

```
caching-benchmark/
  run.sh                     # orchestrator: docker cluster + workload + scale/failure event
  run-multi.sh               # multi-client near-cache/bloom harness (N clients, disjoint key slices)
  aggregate-multi.py         # sums per-client JSONs into one summary line (+ split read/write latencies)
  docker-compose.yml         # optional steady-state cluster (run.sh uses plain `docker`)
  client/                    # Hot Rod workload driver (Maven, shaded jar)
  results/                   # per-run JSON + memory snapshots (git-ignored)
```

## Prerequisites

- Docker (single host; the cluster uses host networking + the default multicast stack)
- JDK 21 and Maven
- A `quay.io/infinispan/server:<tag>` image (default `16.3`; keep the tag in `run.sh`
  and `client/pom.xml` in sync — the client serializes the cache config in its own
  version, so the two should be on the same minor release)

## Build the client

```bash
cd client
mvn -q -DskipTests package      # -> target/caching-bench.jar
```

## Run a step

```bash
# Step 1: single owner, 3 nodes, uniform keys, 60s run
./run.sh --step 1 --nodes 3 --duration 60

# Step 2: single owner + client near cache (near cache is on for step 2).
# Tune the key mix for the hot-key story:
./run.sh --step 2 --nodes 3 --duration 60 \
  --workload "--distribution zipf --read-ratio 0.95 --keys 100000 --threads 8"

# Step 3: single owner + near cache + bloom (all on for step 3)
./run.sh --step 3 --nodes 3 --duration 60 \
  --workload "--distribution zipf --read-ratio 0.9 --keys 100000 --threads 8"
```

`--workload "..."` appends to the step's cache-shape flags and can override any of
them (e.g. `--workload "--owners 2"`).

`run.sh` starts the cluster, waits for it, runs the workload, and writes
`results/step<N>-<time>.json` (throughput/latency/hits) plus memory snapshots.

### Scale / failure events (rebalance & recovery)

```bash
# add a node at t=40s -> watch the throughput/latency dip and recovery
./run.sh --step 0 --nodes 3 --duration 90 --scale-at 40

# kill a node at t=40s (step 4: entries re-obtained from the store)
./run.sh --step 4 --nodes 3 --duration 90 --kill-at 40
```

The per-second time series in the JSON is what you plot to show the dip.

### Multi-client near-cache benchmark (`run-multi.sh`)

N separate client JVMs against one shared cluster, each caching a **disjoint slice** of the
keyspace (client *i* owns keys `[i·keys/N, (i+1)·keys/N)`); an init process creates the cache
and prefills all keys so every worker starts warm. This is where near-cache invalidation fan-out —
with or without the bloom filter — shows up:

```bash
# 8 clients, no bloom; repeat with --bloom to compare selective vs full fan-out
./run-multi.sh --clients 8 --label multi8-nb
./run-multi.sh --clients 8 --bloom --label multi8-bloom

# shrink the near caches below their slices so reads miss and actually hit the server
./run-multi.sh --clients 8 --near-cache-frac 4 --label multi8-fc4
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--clients <n>` | 8 | parallel client JVMs, each with a disjoint key slice |
| `--threads <n>` | 4 | worker threads per client (total concurrency = clients × threads) |
| `--near-cache-frac <f>` | 1 | near cache size = slice/f; f>1 makes uniform reads miss at ~1−1/f and go remote (see below) |
| `--bloom` | off | enable the near-cache bloom filter (selective invalidation, ISPN-5545) |

Everything else (`--keys`, `--read-ratio`, `--distribution`, …) is a regular workload knob.
Per-client JSONs land in `results/<label>-c<i>.json`; `aggregate-multi.py` prints one summary line
plus a per-client breakdown (run automatically at the end).

### How to read the numbers (design notes)

**Headline ops/s is write-gated by default.** With `--near-cache-frac 1` each near cache holds its
whole slice and hit rate stays ≈100%, so reads are local map lookups that never touch the cluster.
Each worker thread alternates fast local reads with RTT-bound writes, so at readRatio 0.5:

```
AGG ops/s ≈ reads + writes = 2 × (cluster write capacity)
```

Scaling clients or nodes therefore moves this number only as far as **write** throughput goes —
that is the intended behavior of this metric with full-size near caches, not a bug in the harness.
Use `--near-cache-frac N` when you want reads to actually exercise the server's read path (uniform
reads then miss at ~1−1/N and pay network RTT).

Per-client JSON fields:

| Field | Meaning |
|-------|---------|
| `latencyUs` | combined read+write histogram (legacy view, kept for old plots) |
| `readLatencyUs` / `writeLatencyUs` | split histograms — the primary locality signal: a near-cache-served read is ~µs while a remote one pays network RTT. A control run with `--near-cache none` gives the pure-remote baseline to compare against (measured ≈14–35µs on this host) |
| `readValueMisses` | reads where the *server* returned null — in a prefilled benchmark this stays ~0 and says nothing about locality; do not use it as a hit-rate proxy |
| `nearCacheHits` / `nearCacheMisses` / `remoteReads` | true near-cache locality counters read at exit from Hot Rod's built-in stats (`RemoteCache.clientStatistics()`); present in every run with a near cache. With an invalidated near cache, `remoteReads` = misses = reads that went to the server (e.g. verified ≈75% remote for uniform access into a ¼-size near cache) |
| `correctedReadLatencyUs` / `correctedWriteLatencyUs` | coordinated-omission-corrected split histograms — what a steady arrival rate would have seen during stalls; see below. Absent in older result JSONs |
| `readExpectedIntervalUs` / `writeExpectedIntervalUs` | the expected inter-sample interval E used for that correction: per-thread median wall-clock gap between consecutive same-type ops, in µs (`0.00` = fewer than two gaps captured → corrected block equals raw) |

Old result JSONs predate these fields and only have the combined view.

**Percentiles come in two flavors: raw and coordinated-omission-corrected.** The workers are
closed-loop — each thread issues the next op only after the previous one returns, so while the system
stalls (GC pause, rebalance, invalidation storm) *no* requests are issued at all. Raw percentiles then
snap back to "fast" as if nothing happened: in the classic example of a client sending 10 req/s that hits
a 5 s pause once per minute, ~98% of samples are fast and p99 looks fine even though roughly one request
in fifty was stuck for five seconds. The `corrected*` blocks re-inject synthetic samples using HdrHistogram's
coordinated-omission correction with E = each thread's median wall-clock gap between consecutive same-type
ops (the expected interval under sustained load; the median is robust to warmup and one-off stalls).
Properties: corrected percentiles ≥ raw, `max` unchanged (synthetic values are always smaller than their
parent sample), and when a run has no pauses the two views agree. Use *corrected* p99/p999 when comparing
runs that differ in how often they stall; use *raw* for steady-state latency of individual ops.
`aggregate-multi.py` prints both (old result JSONs have no corrected blocks and show only the raw view).

**Test crosstalk between concurrent client runs (expected interference):**

- **Invalidation fan-out.** Each client registers a near-cache listener on its own Hot Rod
  connection, so without `--bloom` *every* write by any client is pushed over the wire to all other
  N−1 clients — even though disjoint slices mean those notifications remove nothing. With `--bloom`,
  each client uploads a bloom filter of the keys it actually caches and the server only pushes
  "maybe I have this key", dropping fan-out from O(N) per write to ≈1 + false positives (ISPN-5545).
- **Shared host CPU.** All N client JVMs plus the cluster run on one box. Past runs show p99 rising
  ~3× going 8→32 clients and AGG ops/s *falling* as clients are added — that is scheduling contention,
  not cache behavior. When comparing bloom vs no-bloom or scaling client count, isolate CPUs per JVM
  (`taskset -c` / docker `--cpuset-cpus`) so fan-out cost isn't confounded with scheduling noise; the
  alternative (fewer clients × more threads) keeps total concurrency equal with less JVM overhead.
- **Value-level "hit rate" cannot prove locality.** In a prefilled benchmark every key exists, so
  reads return non-null whether served by the near cache or remotely and `readHitRate` stays ≈100%
  either way. Judge read locality from the split histograms (local ~µs vs remote RTT) against a
  `--near-cache none` control run, or read the exact split from `nearCacheHits/nearCacheMisses`.
  A drop in value-level hit rate below ≈100% means keys are genuinely missing —
  investigate (wrong slice, evictions beyond the configured size, failed prefill).

**Pinned matrix results (bloom vs no-bloom, two repeats each).** With `--pin-cpus 1` (one dedicated core per client JVM) at 8 and 16 clients on a 3-node cluster: repeat-to-repeat variance in AGG ops/s reached ~±13% even pinned, while the bloom/no-bloom difference stayed *inside* that band at both client counts — so **selective invalidation shows no measurable benefit below ≈16 clients here**: per-write RTT + server processing dominates over O(N−1) wasted push messages. The old unpinned "AGG falls as clients are added" trend was mostly scheduling contention (the pinned no-bloom 8-client pair agrees within ~2%, though host-level variance still reached ±13% in other cells). Coordinated-omission-corrected p99s expose multi-ms stalls in every run that raw percentiles understate (~3–6 ms sustained-pressure tail vs <1 ms raw combined).

## Knobs

Workload shape (pass via `--workload "..."`):

| Flag | Default | Meaning |
|------|---------|---------|
| `--keys` | 200000 | working-set size |
| `--value-size` | 1024 | value size in bytes (String) |
| `--read-ratio` | 0.9 | fraction of reads (rest are writes) |
| `--distribution` | uniform | `uniform` or `zipf` (hot keys) |
| `--threads` | 8 | client concurrency |
| `--warmup` | 10 | seconds to discard before measuring |
| `--db-latency-ms` | 0 | simulated system-of-record load on a miss |

Cache shape (set per step by `run.sh`, overridable via `--workload "..."`):

| Flag | Meaning |
|------|---------|
| `--owners <n>` | owners for the distributed cache (2 = baseline, 1 = single owner) |
| `--persistence` | add a single-file store (stand-in for a system of record) |
| `--near-cache <nc>` | `none` or `invalidated` (client-side) |
| `--near-cache-size <n>` | max near-cache entries (default 100000) |
| `--bloom` | enable the near-cache bloom filter |

## Reproducing the numbers

Pin the image tag, keep the heap fixed (`--heap`), discard warmup, and run each
step 3-5 times; report the median. State the host spec and image tag in the post's
Methodology.

## Caveats

- Single-host clustering relies on host networking + the default JGroups multicast
  stack. For a user-defined bridge network or Kubernetes, set the cache-container
  `<transport stack="kubernetes">` (or `dns`) and define `jgroups.dnsPing.query`
  (see the *Server container* docs).
- The file store in step 4 is a stand-in for your system of record; swap in a store
  that reads from your database.
- Multi-client runs (`run-multi.sh`) share the host's CPUs across all client JVMs and
  the cluster, so aggregate numbers include scheduling contention — see *How to read
  the numbers* before comparing runs with different client counts or bloom settings.
- The cache is created at runtime in the config schema version of the **client**. Keep
  the client and the server image on the same minor release, or the server may reject
  the config.
