#!/usr/bin/env python3
"""Aggregate a multi-client run: sum per-client throughput and report per-client stats.

Usage: aggregate-multi.py <run-name> <clients> <results-dir>
Reads <results-dir>/<run-name>-c<i>.json for i in [0, clients) and prints one summary
line per run plus a per-client breakdown. The headline number is aggregate ops/s (the sum
of every client's throughput), which is what the near-cache bloom filter is meant to
protect: with it, the server only sends an invalidation to the clients that actually cache
the written key, instead of fanning out to every listener.

Reading the numbers correctly matters here. With a near cache sized to each client's slice,
reads are served locally and AGG ops/s is really ~2x the cluster's *write* capacity (each
worker thread alternates fast local reads with RTT-bound writes). The summary therefore also
reports:
  remoteReads   reads that missed the near cache (= the read share that actually hit the server)
  rP50/wP50     median per-client p50 for reads vs writes (split histograms; absent in old runs)

The second summary line also reports the coordinated-omission-corrected tail when present in
the client JSONs: this driver is closed-loop, so while the system pauses no requests are issued
and raw percentiles understate what sustained load would see; corrected* re-injects synthetic
samples at each thread's median inter-sample interval (see README "How to read the numbers").
Old result JSONs without split latency blocks still show only combined p99, and those without
corrected blocks show only the raw view.
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
    p99 = max(d["latencyUs"]["p99"] for d in ds)          # combined, worst client (legacy view)
    reads = sum(d.get("reads", 0) for d in ds)
    hits = sum(d.get("readHits", 0) for d in ds)
    writes = sum(d.get("writes", 0) for d in ds)
    # True locality is only known when the client exposed near-cache stats (nearCacheMisses);
    # in a prefilled benchmark value-level "hit rate" stays ~100% either way and proves nothing.
    has_nc = any("nearCacheMisses" in d for d in ds)
    remote_reads = sum(d.get("nearCacheMisses", 0) for d in ds) if has_nc else None
    hitrate = 100.0 * hits / reads if reads else 0.0

    bloom = ds[0].get("bloom", False)
    rr = ds[0].get("readRatio", 0)
    tag = "bloom" if bloom else "no-bloom"
    print(f"{run:<24} {tag:<9} clients={clients:<3} readRatio={rr:<4} "
          f"AGG ops/s={int(agg_ops):>8}  per-client={int(sum(per_ops)/len(per_ops)):>8} "
          f"(min {int(min(per_ops))} / max {int(max(per_ops))})  "
          f"p99(combined)={int(p99):>7}us (worst)  hitRate*={hitrate:5.1f}%  "
          f"reads={int(reads)} writes={int(writes)} remoteReads={'n/a' if remote_reads is None else int(remote_reads)}")

    # Split read/write latency view, when the client emitted separate histograms.
    if any("writeLatencyUs" in d for d in ds):
        r50 = [d["readLatencyUs"]["p50"] for d in ds]
        w50 = [d["writeLatencyUs"]["p50"] for d in ds]
        wp99 = max(d["writeLatencyUs"]["p99"] for d in ds)
        rp99 = max(d["readLatencyUs"]["p99"] for d in ds)
        print(f"{'':<24} read p50={sum(r50)/len(r50):8.1f}us (avg client, worst {max(r50):7.1f})  "
              f"write p50={sum(w50)/len(w50):8.1f}us (worst)  write p99={int(wp99):>6}us  read p99={int(rp99):>6}us")

    # Coordinated-omission-corrected tail (see docstring; emitted by clients built after the
    # gap-ring change). E~ is each client's median inter-sample wall-clock gap for that op type.
    if any("correctedWriteLatencyUs" in d for d in ds):
        crp99 = max(d["correctedReadLatencyUs"]["p99"] for d in ds)
        cwp99 = max(d["correctedWriteLatencyUs"]["p99"] for d in ds)
        rei = sum(d.get("readExpectedIntervalUs", 0.0) for d in ds) / len(ds)
        wei = sum(d.get("writeExpectedIntervalUs", 0.0) for d in ds) / len(ds)
        print(f"{'':<24} corrected read p99={int(crp99):>6}us (worst, E~{rei:7.2f}us)   "
              f"corrected write p99={int(cwp99):>6}us (worst, E~{wei:7.2f}us)")

    # per-client breakdown
    for i, d in enumerate(ds):
        line = f"    c{i:<2} ops/s={int(d['opsPerSec']):>8}  p50={d['latencyUs']['p50']:>8.2f}us  " \
               f"p99={d['latencyUs']['p99']:>8.2f}us  hitRate={100*d['readHitRate']:5.1f}%"
        if "writeLatencyUs" in d:
            line += (f"   rP50={d['readLatencyUs']['p50']:7.1f}/wP50={d['writeLatencyUs']['p50']:8.1f}us")
        print(line)

if __name__ == "__main__":
    main()
