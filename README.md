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
caching/
  run.sh                     # orchestrator: docker cluster + workload + scale/failure event
  docker-compose.yml         # optional steady-state cluster (run.sh uses plain `docker`)
  client/                    # Hot Rod workload driver (Maven, shaded jar)
  results/                   # per-run JSON + memory snapshots (git-ignored)
```

## Prerequisites

- Docker (single host; the cluster uses host networking + the default multicast stack)
- JDK 21 and Maven
- A `quay.io/infinispan/server:<tag>` image (default `16.2`; keep the tag in `run.sh`
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
- The cache is created at runtime in the config schema version of the **client**. Keep
  the client and the server image on the same minor release, or the server may reject
  the config.
