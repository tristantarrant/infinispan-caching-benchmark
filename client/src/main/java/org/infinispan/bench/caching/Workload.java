package org.infinispan.bench.caching;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Hot Rod workload driver for the caching-workload benchmark series.
 *
 * <p>Phases: (A) an optional one-shot prefill that populates the working set so the
 * steady state is a warm cache; (B+C) a single worker run spanning warmup + measure,
 * issuing a mix of reads and writes. Throughput is reported over the measure window
 * only (via the per-second series); the latency histogram spans warmup + measure so it
 * reflects steady state.
 */
public final class Workload {

   public static void main(String[] argv) throws Exception {
      if (argv.length == 0) {
         Args.help(System.out);
         return;
      }
      Args a = Args.parse(argv);
      if (a.bool("help")) {
         Args.help(System.out);
         return;
      }
      if (a.str("hosts", "").isBlank()) {
         System.err.println("error: --hosts is required");
         Args.help(System.err);
         System.exit(2);
      }

      int warmup = a.intVal("warmup", 10);
      int duration = a.intVal("duration", 60);
      int keys = a.intVal("keys", 200_000);
      int valueSize = Math.max(1, a.intVal("value-size", 1024));
      double readRatio = a.dbl("read-ratio", 0.9);
      int threads = Math.max(1, a.intVal("threads", 8));
      boolean zipf = "zipf".equalsIgnoreCase(a.str("distribution", "uniform"));
      long dbLatencyMs = a.lng("db-latency-ms", 0);
      boolean prefill = !a.bool("no-prefill");
      String cacheName = a.str("cache", "cache");
      String hosts = a.str("hosts", "");

       RemoteCacheManager rcm = ClientConfig.create(a);
       try {
          System.out.printf(">>> %s cache '%s' (owners=%d, persistence=%b)%n",
             a.bool("no-create-cache") ? "attaching to" : "creating",
             cacheName, a.intVal("owners", 2), a.bool("persistence"));
          RemoteCache cache = CacheFactory.create(rcm, a);
          String value = "v".repeat(valueSize);

          int clientIndex = Math.max(0, a.intVal("client-index", 0));
          int clientCount = Math.max(1, a.intVal("client-count", 1));
          int slice = Math.max(1, keys / clientCount);
          int base = clientIndex * slice;

          if (a.bool("init-only")) {
             // single process: create the cache (done above) and prefill the whole
             // keyspace so every worker starts against a warm remote cache, then exit
             if (prefill) {
                System.out.printf(">>> init-only: prefilling %d keys with %dB values%n", keys, valueSize);
                long t0 = System.nanoTime();
                for (int i = 0; i < keys; i++) cache.put(i, value);
                System.out.printf(">>> init-only done in %.1fs%n", (System.nanoTime() - t0) / 1e9);
             }
             System.out.println(">>> init-only complete; cache ready");
             return;
          }

          KeyGen keyGen = new KeyGen(slice, zipf);
          if (clientCount > 1) {
             System.out.printf(">>> client %d/%d owns key slice [%d, %d) of %d%n",
                clientIndex, clientCount, base, base + slice, keys);
          }

          if (prefill) {
             System.out.printf(">>> prefilling %d keys (own slice) with %dB values%n", slice, valueSize);
             long t0 = System.nanoTime();
             for (int i = 0; i < slice; i++) cache.put(base + i, value);
             System.out.printf(">>> prefill done in %.1fs%n", (System.nanoTime() - t0) / 1e9);
          }

         Metrics metrics = new Metrics(threads, warmup + duration + 2);
         long stopAfterNanos = TimeUnit.SECONDS.toNanos(warmup + duration);

         CountDownLatch start = new CountDownLatch(1);
         CountDownLatch done = new CountDownLatch(threads);
         Thread[] workers = new Thread[threads];
         for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
               try {
                  start.await();
                  long stopNanos = System.nanoTime() + stopAfterNanos;
                   while (System.nanoTime() < stopNanos) {
                      int keyIdx = base + keyGen.nextKey();
                      if (ThreadLocalRandom.current().nextDouble() < readRatio) {
                        long s0 = System.nanoTime();
                        Object v = cache.get(keyIdx);
                        long opNs = System.nanoTime() - s0;
                        if (v == null) {
                           metrics.recordReadMiss();
                           if (dbLatencyMs > 0) Thread.sleep(dbLatencyMs);
                           metrics.recordDbLoad();
                        } else {
                           metrics.recordReadHit();
                        }
                        // Reads are recorded separately: a hit may be served from the local
                        // near cache (no network), while every write goes to the server.
                        metrics.recordReadLatency(tid, opNs);
                     } else {
                        long s0 = System.nanoTime();
                        cache.put(keyIdx, value);
                        long opNs = System.nanoTime() - s0;
                        metrics.recordWrite();
                        metrics.recordWriteLatency(tid, opNs);
                     }
                     metrics.recordOp((int) metrics.elapsedSecond());
                  }
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } catch (Exception e) {
                  System.err.println("worker-" + tid + " failed: " + e);
                  e.printStackTrace(System.err);
                  throw new RuntimeException(e);
               } finally {
                  done.countDown();
               }
            }, "worker-" + t);
         }

         for (Thread w : workers) w.start();
         start.countDown();
         System.out.printf(
            ">>> warmup %ds, measure %ds, %d threads, distribution=%s, readRatio=%.2f, near-cache=%s, bloom=%b%n",
            warmup, duration, threads, zipf ? "zipf" : "uniform", readRatio,
            a.str("near-cache", "none"), a.bool("bloom"));
         done.await();
         for (Thread w : workers) w.join();

         long[] ncStats = nearCacheStats(cache);
         String json = toJson(a, metrics, warmup, duration, keys, valueSize, threads,
            zipf, readRatio, dbLatencyMs, hosts, cacheName, ncStats);
         System.out.println(json);

         String resultDir = a.str("result-dir", "");
         if (!resultDir.isBlank()) {
            Path dir = Path.of(resultDir);
            Files.createDirectories(dir);
            Path out = dir.resolve(a.str("run-name", "run") + ".json");
            Files.writeString(out, json);
            System.out.println(">>> wrote " + out.toAbsolutePath());
         }
      } finally {
         rcm.stop();
      }
   }

   private static String toJson(Args a, Metrics m, int warmup, int duration, int keys, int valueSize,
                                int threads, boolean zipf, double readRatio, long dbLatencyMs,
                                String hosts, String cacheName,
                                 long[] ncStats) {
      long[] series = m.opsSeries(warmup, duration);
      long measuredOps = 0;
      for (long v : series) measuredOps += v;
      double opsPerSec = duration > 0 ? (double) measuredOps / duration : 0;
      // Split latency views: reads vs writes. With an invalidated near cache sized to the
      // working set, read p50 can be local (microseconds) while every write pays network RTT;
      // a merged histogram hides that and makes "ops/s" look like cluster throughput when it
      // is really gated by write processing; see "How to read the numbers" in the README for details.
      double[] lat = m.percentilesUs();
      double[] rlat = m.percentilesUs(Metrics.READS);
      double[] wlat = m.percentilesUs(Metrics.WRITES);

      StringBuilder sb = new StringBuilder();
      sb.append("{\n");
      sb.append("  \"step\": ").append(a.intVal("step", -1)).append(",\n");
      sb.append("  \"cacheSpec\": ").append(jsonStr(cacheSpec(a))).append(",\n");
      sb.append("  \"owners\": ").append(a.intVal("owners", 2)).append(",\n");
      sb.append("  \"persistence\": ").append(a.bool("persistence")).append(",\n");
      sb.append("  \"nodes\": ").append(a.intVal("nodes", 0)).append(",\n");
      sb.append("  \"hosts\": ").append(jsonStr(hosts)).append(",\n");
       sb.append("  \"cache\": ").append(jsonStr(cacheName)).append(",\n");
       sb.append("  \"clientIndex\": ").append(a.intVal("client-index", 0)).append(",\n");
       sb.append("  \"clientCount\": ").append(a.intVal("client-count", 1)).append(",\n");
       sb.append("  \"slice\": ").append(Math.max(1, a.intVal("keys", 200_000) / Math.max(1, a.intVal("client-count", 1)))).append(",\n");
        sb.append("  \"runName\": ").append(jsonStr(a.str("run-name", ""))).append(",\n");
       sb.append("  \"label\": ").append(jsonStr(a.str("label", ""))).append(",\n");
       sb.append("  \"scaleAt\": ").append(a.intVal("scale-at", 0)).append(",\n");
       sb.append("  \"killAt\": ").append(a.intVal("kill-at", 0)).append(",\n");
      sb.append("  \"distribution\": ").append(jsonStr(zipf ? "zipf" : "uniform")).append(",\n");
      sb.append("  \"readRatio\": ").append(readRatio).append(",\n");
      sb.append("  \"keys\": ").append(keys).append(",\n");
      sb.append("  \"valueSize\": ").append(valueSize).append(",\n");
      sb.append("  \"threads\": ").append(threads).append(",\n");
      sb.append("  \"warmupSeconds\": ").append(warmup).append(",\n");
      sb.append("  \"durationSeconds\": ").append(duration).append(",\n");
      sb.append("  \"nearCache\": ").append(jsonStr(a.str("near-cache", "none"))).append(",\n");
      sb.append("  \"nearCacheSize\": ").append(a.intVal("near-cache-size", 0)).append(",\n");
      sb.append("  \"bloom\": ").append(a.bool("bloom")).append(",\n");
      sb.append("  \"dbLatencyMs\": ").append(dbLatencyMs).append(",\n");
      sb.append("  \"measuredOps\": ").append(measuredOps).append(",\n");
      sb.append("  \"opsPerSec\": ").append(round2(opsPerSec)).append(",\n");
      sb.append("  \"reads\": ").append(m.reads()).append(",\n");
      sb.append("  \"writes\": ").append(m.writes()).append(",\n");
      sb.append("  \"readHits\": ").append(m.readHits()).append(",\n");
      sb.append("  \"readMisses\": ").append(m.readMisses()).append(",\n");
      sb.append("  \"readHitRate\": ").append(round4(m.readHitRate())).append(",\n");
      // Value-level misses (the server returned null). In a prefilled benchmark this stays ~0 and
      // says nothing about locality; the near-cache counters below measure that.
      sb.append("  \"readValueMisses\": ").append(m.readMisses()).append(",\n");
      if (ncStats != null) {
         sb.append("  \"nearCacheHits\": ").append(ncStats[0]).append(",\n");
         sb.append("  \"nearCacheMisses\": ").append(ncStats[1]).append(",\n");
         // With an invalidated near cache, a miss means the read went remote.
         sb.append("  \"remoteReads\": ").append(ncStats[1]).append(",\n");
      }
      sb.append("  \"dbLoads\": ").append(m.dbLoads()).append(",\n");
      appendLatencyBlock(sb, "latencyUs", lat, m.meanUs());
      appendLatencyBlock(sb, "readLatencyUs", rlat, m.meanUs(Metrics.READS));
      appendLatencyBlock(sb, "writeLatencyUs", wlat, m.meanUs(Metrics.WRITES));

      // Coordinated-omission correction. This is a closed-loop load tester: when the system stalls,
      // in-flight ops drop to zero and raw percentiles understate tail latency (the classic
      // 10 req/s + 5 s pause trap). The blocks below re-inject synthetic samples at the expected
      // inter-sample interval - the median measured wall-clock gap between consecutive same-type
      // ops on each thread. A value of 0 means fewer than two gaps were captured (no correction);
      // in that case the corrected block equals the raw one. Raw blocks above are unchanged.
      double[] crlat = m.correctedPercentilesUs(Metrics.READS);
      double[] cwlat = m.correctedPercentilesUs(Metrics.WRITES);
      sb.append("  \"readExpectedIntervalUs\": ").append(round2(m.expectedIntervalNs(Metrics.READS) / 1000.0)).append(",\n");
      sb.append("  \"writeExpectedIntervalUs\": ").append(round2(m.expectedIntervalNs(Metrics.WRITES) / 1000.0)).append(",\n");
      appendLatencyBlock(sb, "correctedReadLatencyUs", crlat, m.correctedMeanUs(Metrics.READS));
      appendLatencyBlock(sb, "correctedWriteLatencyUs", cwlat, m.correctedMeanUs(Metrics.WRITES));
      sb.append("  \"opsPerSecond\": [");
      for (int i = 0; i < series.length; i++) {
         if (i > 0) sb.append(",");
         sb.append(series[i]);
      }
      sb.append("]\n");
      sb.append("}\n");
      return sb.toString();
   }

   /** Reads the Hot Rod client's built-in near-cache counters ({@link ClientStatistics}); returns
    * {hits, misses} or null when this cache/ version does not expose them. */
   private static long[] nearCacheStats(RemoteCache cache) {
      try {
         if (cache instanceof InternalRemoteCache irc && irc.clientStatistics() != null) {
            ClientStatistics cs = irc.clientStatistics();
            return new long[]{cs.getNearCacheHits(), cs.getNearCacheMisses()};
         }
      } catch (Exception e) {
         System.err.println(">>> could not read client near-cache stats: " + e);
      }
      return null;
   }

   /** Emits a {@code <name>: {p50, p90, p99, p999, max, mean}} JSON block in microseconds. */
   private static void appendLatencyBlock(StringBuilder sb, String name, double[] lat, double mean) {
      sb.append("  \"").append(name).append("\": {\n");
      sb.append("    \"p50\": ").append(round2(lat[0])).append(",\n");
      sb.append("    \"p90\": ").append(round2(lat[1])).append(",\n");
      sb.append("    \"p99\": ").append(round2(lat[2])).append(",\n");
      sb.append("    \"p999\": ").append(round2(lat[3])).append(",\n");
      sb.append("    \"max\": ").append(round2(lat[4])).append(",\n");
      sb.append("    \"mean\": ").append(round2(mean)).append("\n");
      sb.append("  },\n");
   }

   /** A single-line description of the cache + near-cache shape for the JSON output. */
   private static String cacheSpec(Args a) {
      StringBuilder sb = new StringBuilder("owners=").append(a.intVal("owners", 2));
      if (a.bool("persistence")) sb.append(", persistence");
      String nc = a.str("near-cache", "none");
      if (!"none".equalsIgnoreCase(nc)) {
         sb.append(", near-cache=").append(nc).append("(size=").append(a.intVal("near-cache-size", 0)).append(")");
         if (a.bool("bloom")) sb.append("+bloom");
      }
      return sb.toString();
   }

   private static String jsonStr(String s) {
      StringBuilder sb = new StringBuilder("\"");
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         switch (c) {
            case '"' -> sb.append("\\\"");
            case '\\' -> sb.append("\\\\");
            case '\n' -> sb.append("\\n");
            case '\r' -> sb.append("\\r");
            case '\t' -> sb.append("\\t");
            default -> sb.append(c);
         }
      }
      return sb.append('"').toString();
   }

   private static String round2(double d) {
      return String.format("%.2f", d);
   }

   private static String round4(double d) {
      return String.format("%.4f", d);
   }

   private Workload() {
   }
}
