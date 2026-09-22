#!/usr/bin/env python3
"""Aggregate a multi-client run: sum per-client throughput and report per-client stats.

Usage: aggregate-multi.py <run-name> <clients> <results-dir>
Reads <results-dir>/<run-name>-c<i>.json for i in [0, clients) and prints one summary
line per run plus a per-client breakdown. The headline number is aggregate ops/s
(the sum of every client's throughput), which is what the near-cache bloom filter is
meant to protect: with it, the server only sends an invalidation to the clients that
actually cache the written key, instead of fanning out to every listener.
"""
import json, os, sys

def load(res, name):
    f = os.path.join(res, name)
    if not os.path.exists(f):
        return None
    return json.load(open(f))

def main():
    run, clients, res = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    ds = [load(res, f"{run}-c{i}.json") for i in range(clients)]
    missing = [i for i, d in enumerate(ds) if d is None]
    if missing:
        print(f"{run}: MISSING client jsons for indices {missing}")
        return

    agg_ops = sum(d["opsPerSec"] for d in ds)
    per_ops = [d["opsPerSec"] for d in ds]
    p99 = [d["latencyUs"]["p99"] for d in ds]
    p50 = [d["latencyUs"]["p50"] for d in ds]
    reads = sum(d["reads"] for d in ds)
    hits = sum(d["readHits"] for d in ds)
    writes = sum(d["writes"] for d in ds)
    hitrate = 100.0 * hits / reads if reads else 0.0

    bloom = ds[0].get("bloom", False)
    rr = ds[0].get("readRatio", 0)
    tag = "bloom" if bloom else "no-bloom"
    print(f"{run:<24} {tag:<9} clients={clients:<3} readRatio={rr:<4} "
          f"AGG ops/s={int(agg_ops):>8}  per-client={int(sum(per_ops)/len(per_ops)):>8} "
          f"(min {int(min(per_ops))} / max {int(max(per_ops))})  "
          f"p99={int(max(p99)):>7}us (worst)  hitRate={hitrate:5.1f}%  reads={int(reads)} writes={int(writes)}")

    # per-client breakdown
    for i, d in enumerate(ds):
        print(f"    c{i:<2} ops/s={int(d['opsPerSec']):>8}  p50={d['latencyUs']['p50']:>8.2f}us  "
              f"p99={d['latencyUs']['p99']:>8.2f}us  hitRate={100.0*d['readHitRate']:5.1f}%")

if __name__ == "__main__":
    main()
