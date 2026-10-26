package org.infinispan.bench.caching;

import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Client-side metrics: per-thread latency histograms (merged on read) for reads and
 * writes <em>separately</em>, op counters, read hit-rate, and a per-second op time series
 * used to plot throughput dips around a scale/failure event.
 *
 * Percentiles can also be read after coordinated-omission correction ({@link #correctedPercentilesUs}): closed-loop workers stop issuing ops while the system stalls, so raw percentiles understate tail latency; E is each thread median measured inter-sample gap (see README).
 *
 * <p>The split matters because with an invalidated near cache sized to the working set,
 * "reads" are local map lookups while only misses (and all writes) touch the network; one
 * merged histogram hides which side of the wire is slow. With an invalidated near cache a
 * miss means the read went remote, so {@link #readMisses()} equals the number of <em>remote</em>
 * reads and {@code 1 - hitRate} is the fraction of ops that actually exercised the cluster.
 */
public final class Metrics {
   private static final long MAX_VALUE_NS = 60_000_000_000L; // 60s ceiling

   /** Histogram op-type index: reads first, writes second. */
   public static final int READS = 0;
   public static final int WRITES = 1;

   /** Ring size of inter-sample gap samples kept per thread (circular; median at exit). */

   private static final int GAP_RING = 1024;

   private final AtomicLongArray opsPerSecond;
   /** Per-thread latency histograms: [opType][thread]. */
   private final Histogram[][] histograms;
   // Inter-sample gap rings for coordinated-omission correction (see expectedIntervalNs). Layout per op type:
   // flat ring of threads*GAP_RING slots, slot = thread * GAP_RING + pos. Each worker only ever writes its own
   // slice, so no synchronization is needed (main reads after join()).
   private long[][] gapRings;      // [opType][threads * GAP_RING]
   private int[] gapPos;           // next write position per op type slot group: opType*threads + thread
   private long[] lastSampleNs;    // wall-clock ns of previous sample, same layout as gapPos
   /** Worker thread count (for flat-ring indexing). */
   private final int threads;
   private final AtomicLong reads = new AtomicLong();
   private final AtomicLong writes = new AtomicLong();
   private final AtomicLong readHits = new AtomicLong();
   private final AtomicLong readMisses = new AtomicLong();
   private final AtomicLong dbLoads = new AtomicLong();
   private final long startNanos;

   public Metrics(int threads, int seriesSize) {
      this.opsPerSecond = new AtomicLongArray(seriesSize);
      this.histograms = new Histogram[2][threads];
      for (int i = 0; i < threads; i++) {
         histograms[READS][i] = new Histogram(MAX_VALUE_NS, 3);
         histograms[WRITES][i] = new Histogram(MAX_VALUE_NS, 3);
      }
      this.gapRings = new long[2][threads * GAP_RING];
      this.gapPos = new int[2 * threads];
      this.lastSampleNs = new long[2 * threads];
      Arrays.fill(this.lastSampleNs, -1L); // first sample per slot has no gap to record yet
      this.threads = threads;
      this.startNanos = System.nanoTime();
   }

   /** Whole seconds elapsed since this metrics instance was created. */
   public long elapsedSecond() {
      return (System.nanoTime() - startNanos) / 1_000_000_000L;
   }

   public void recordOp(int second) {
      if (second >= 0 && second < opsPerSecond.length()) opsPerSecond.getAndIncrement(second);
   }

   /** Records a read latency in the per-thread reads histogram. */
   public void recordReadLatency(int thread, long ns) {
      captureGap(READS, thread);
      record(histograms[READS][thread], ns);
   }

   /** Records a write latency in the per-thread writes histogram. */
   public void recordWriteLatency(int thread, long ns) {
      captureGap(WRITES, thread);
      record(histograms[WRITES][thread], ns);
   }

   private static void record(Histogram h, long ns) {
      if (ns < 0) ns = 0;
      if (ns > MAX_VALUE_NS) ns = MAX_VALUE_NS;
      h.recordValue(ns);
   }

      /**
    * Captures the wall-clock gap since this thread's previous sample of the same op type. In a closed-loop
    * worker that gap spans one full cycle (the op plus any interleaved ops), so its median approximates
    * HdrHistogram's "expected interval between value samples" under sustained load; used at exit only, for
    * coordinated-omission correction of the percentiles below. No synchronization: each slot is written by
    * exactly one worker thread and read after join().
   */
   private void captureGap(int opType, int thread) {
      long now = System.nanoTime();
      int idx = opType * threads + thread;
      long last = lastSampleNs[idx];
      if (last >= 0L) {
         gapRings[opType][thread * GAP_RING + gapPos[idx]] = now - last;
         int next = gapPos[idx] + 1;
         if (next == GAP_RING) next = 0;
         gapPos[idx] = next;
      }
      lastSampleNs[idx] = now;
   }

   /** Median inter-sample wall-clock gap per op type, in nanoseconds (the expected interval for coordinated-omission correction); 0 when fewer than two gaps were captured. */
   public long expectedIntervalNs(int opType) {
      int n = 0;
      long[] vals = new long[threads * GAP_RING];
      for (long g : gapRings[opType]) if (g > 0L) vals[n++] = g;
      if (n < 2) return 0L;
      Arrays.sort(vals, 0, n);
      return vals[n / 2];
   }

   /** Merged histogram for one op type after coordinated-omission correction ({@link #expectedIntervalNs} as the expected interval). */
   private Histogram corrected(int opType) {
      long e = expectedIntervalNs(opType);
      Histogram m = merged(opType);
      return (e > 0L) ? m.copyCorrectedForCoordinatedOmission(e) : m;
   }

   /** {@code [p50, p90, p99, p999, max]} in microseconds for one op type after coordinated-omission correction. */
   public double[] correctedPercentilesUs(int opType) {
      Histogram h = corrected(opType);
      return new double[]{
         us(h.getValueAtPercentile(50)),
         us(h.getValueAtPercentile(90)),
         us(h.getValueAtPercentile(99)),
         us(h.getValueAtPercentile(99.9)),
         us(h.getMaxValue())
      };
   }

   /** Mean latency in microseconds for one op type after coordinated-omission correction. */
   public double correctedMeanUs(int opType) {
      return us(corrected(opType).getMean());
   }
   public void recordReadHit() { reads.getAndIncrement(); readHits.getAndIncrement(); }
   public void recordReadMiss() { reads.getAndIncrement(); readMisses.getAndIncrement(); }
   public void recordWrite() { writes.getAndIncrement(); }
   public void recordDbLoad() { dbLoads.getAndIncrement(); }

   public long reads() { return reads.get(); }
   public long writes() { return writes.get(); }
   public long readHits() { return readHits.get(); }
   /** With an invalidated near cache this is also the number of <em>remote</em> reads. */
   public long readMisses() { return readMisses.get(); }
   public long dbLoads() { return dbLoads.get(); }

   public double readHitRate() {
      long r = reads.get();
      return r == 0 ? 0 : (double) readHits.get() / r;
   }

   /** {@code [p50, p90, p99, p999, max]} in microseconds for one op type, merged across threads. */
   public double[] percentilesUs(int opType) {
      Histogram merged = merged(opType);
      return new double[]{
         us(merged.getValueAtPercentile(50)),
         us(merged.getValueAtPercentile(90)),
         us(merged.getValueAtPercentile(99)),
         us(merged.getValueAtPercentile(99.9)),
         us(merged.getMaxValue())
      };
   }

   /** {@code [p50, p90, p99, p999, max]} in microseconds for reads + writes merged (legacy view). */
   public double[] percentilesUs() {
      Histogram merged = new Histogram(MAX_VALUE_NS, 3);
      for (int i = 0; i < histograms[READS].length; i++) {
         merged.add(histograms[READS][i]);
         merged.add(histograms[WRITES][i]);
      }
      return new double[]{
         us(merged.getValueAtPercentile(50)),
         us(merged.getValueAtPercentile(90)),
         us(merged.getValueAtPercentile(99)),
         us(merged.getValueAtPercentile(99.9)),
         us(merged.getMaxValue())
      };
   }

   /** Mean latency in microseconds for one op type, merged across threads. */
   public double meanUs(int opType) {
      return us(merged(opType).getMean());
   }

   /** Mean latency in microseconds over reads + writes (legacy view). */
   public double meanUs() {
      Histogram merged = new Histogram(MAX_VALUE_NS, 3);
      for (int i = 0; i < histograms[READS].length; i++) {
         merged.add(histograms[READS][i]);
         merged.add(histograms[WRITES][i]);
      }
      return us(merged.getMean());
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

   private Histogram merged(int opType) {
      Histogram merged = new Histogram(MAX_VALUE_NS, 3);
      for (Histogram h : histograms[opType]) merged.add(h);
      return merged;
   }

   private static double us(double ns) {
      return ns / 1000.0;
   }
}
