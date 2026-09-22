package org.infinispan.bench.caching;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread-local key sampler over {@code [0, keySpace)}.
 *
 * <p>{@code uniform} spreads access evenly. {@code zipf} skews it toward a small set of
 * hot keys (alpha = 0.99) by sampling the inverse of a precomputed Zipf CDF, which is the
 * access pattern a near cache is meant to absorb.
 */
public final class KeyGen {
   public static final double ZIPF_ALPHA = 0.99;

   private final int keySpace;
   private final boolean zipf;
   private final double[] cdf;

   public KeyGen(int keySpace, boolean zipf) {
      this.keySpace = keySpace;
      this.zipf = zipf;
      this.cdf = zipf ? buildCdf(keySpace, ZIPF_ALPHA) : null;
   }

   public int nextKey() {
      double u = ThreadLocalRandom.current().nextDouble();
      if (!zipf) return (int) (u * keySpace);
      int lo = 0, hi = keySpace - 1;
      while (lo < hi) {
         int mid = (lo + hi) >>> 1;
         if (cdf[mid] < u) lo = mid + 1;
         else hi = mid;
      }
      return lo;
   }

   private static double[] buildCdf(int n, double alpha) {
      double[] cdf = new double[n];
      double sum = 0;
      for (int i = 1; i <= n; i++) {
         sum += Math.pow(i, -alpha);
         cdf[i - 1] = sum;
      }
      for (int i = 0; i < n; i++) cdf[i] /= sum;
      return cdf;
   }
}
