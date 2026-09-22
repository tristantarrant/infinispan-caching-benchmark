package org.infinispan.bench.caching;

import org.HdrHistogram.Histogram;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Client-side metrics: a per-thread latency histogram (merged on read), op counters,
 * read hit-rate, and a per-second op time series used to plot throughput dips around
 * a scale/failure event.
 */
public final class Metrics {
   private static final long MAX_VALUE_NS = 60_000_000_000L; // 60s ceiling

   private final AtomicLongArray opsPerSecond;
   private final Histogram[] histograms;
   private final AtomicLong reads = new AtomicLong();
   private final AtomicLong writes = new AtomicLong();
   private final AtomicLong readHits = new AtomicLong();
   private final AtomicLong readMisses = new AtomicLong();
   private final AtomicLong dbLoads = new AtomicLong();
   private final long startNanos;

   public Metrics(int threads, int seriesSize) {
      this.opsPerSecond = new AtomicLongArray(seriesSize);
      this.histograms = new Histogram[threads];
      for (int i = 0; i < threads; i++) histograms[i] = new Histogram(MAX_VALUE_NS, 3);
      this.startNanos = System.nanoTime();
   }

   /** Whole seconds elapsed since this metrics instance was created. */
   public long elapsedSecond() {
      return (System.nanoTime() - startNanos) / 1_000_000_000L;
   }

   public void recordOp(int second) {
      if (second >= 0 && second < opsPerSecond.length()) opsPerSecond.getAndIncrement(second);
   }

   public void recordLatency(int thread, long ns) {
      if (ns < 0) ns = 0;
      if (ns > MAX_VALUE_NS) ns = MAX_VALUE_NS;
      histograms[thread].recordValue(ns);
   }

   public void recordReadHit() { reads.getAndIncrement(); readHits.getAndIncrement(); }
   public void recordReadMiss() { reads.getAndIncrement(); readMisses.getAndIncrement(); }
   public void recordWrite() { writes.getAndIncrement(); }
   public void recordDbLoad() { dbLoads.getAndIncrement(); }

   public long reads() { return reads.get(); }
   public long writes() { return writes.get(); }
   public long readHits() { return readHits.get(); }
   public long readMisses() { return readMisses.get(); }
   public long dbLoads() { return dbLoads.get(); }

   public double readHitRate() {
      long r = reads.get();
      return r == 0 ? 0 : (double) readHits.get() / r;
   }

   /** {@code [p50, p90, p99, p999, max]} in microseconds, merged across threads. */
   public double[] percentilesUs() {
      Histogram merged = merged();
      return new double[]{
         us(merged.getValueAtPercentile(50)),
         us(merged.getValueAtPercentile(90)),
         us(merged.getValueAtPercentile(99)),
         us(merged.getValueAtPercentile(99.9)),
         us(merged.getMaxValue())
      };
   }

   public double meanUs() {
      return us(merged().getMean());
   }

   /**
    * Exactly {@code length} per-second op counts starting at {@code fromSecond}.
    * Entries beyond the recorded range are zero-padded, so the returned array always
    * represents the full measurement window.
    */
   public long[] opsSeries(int fromSecond, int length) {
      int start = Math.max(0, Math.min(fromSecond, opsPerSecond.length()));
      long[] out = new long[length];
      for (int i = 0; i < length; i++) {
         int idx = start + i;
         if (idx < opsPerSecond.length()) out[i] = opsPerSecond.get(idx);
      }
      return out;
   }

   private Histogram merged() {
      Histogram merged = new Histogram(MAX_VALUE_NS, 3);
      for (Histogram h : histograms) merged.add(h);
      return merged;
   }

   private static double us(double ns) {
      return ns / 1000.0;
   }
}
