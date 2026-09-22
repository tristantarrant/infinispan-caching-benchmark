package org.infinispan.bench.caching;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Creates the benchmark cache on the running cluster at runtime via the Hot Rod admin
 * API, so no server config file needs to be mounted or version-matched.
 *
 * <p>The shape of the cache is controlled by the workload args:
 * <ul>
 *   <li>{@code --owners} the number of owners (2 = baseline, 1 = single-owner)</li>
   *   <li>{@code --persistence} add a file store (stand-in for a system of record)</li>
 * </ul>
 *
 * <p>A single-owner cache must set partition-handling to {@code ALLOW_READ_WRITES},
 * otherwise {@link ConfigurationBuilder#build()} refuses to validate.
 */
public final class CacheFactory {

    /**
     * Removes any existing cache of the given name and creates a fresh one, or (when
     * {@code --no-create-cache} is set) attaches to the cache that is already there.
     */
    public static RemoteCache create(RemoteCacheManager rcm, Args a) {
       String name = a.str("cache", "cache");
       if (a.bool("no-create-cache")) {
          return rcm.getCache(name);
       }
       int owners = Math.max(1, a.intVal("owners", 2));
      boolean persistence = a.bool("persistence");

      ConfigurationBuilder ccb = new ConfigurationBuilder();
      ccb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ccb.clustering().hash().numOwners(owners);
      if (owners == 1) {
         ccb.clustering().partitionHandling().whenSplit(PartitionHandling.ALLOW_READ_WRITES);
      }
       if (persistence) {
          // Soft-index file store: the one file store the Hot Rod admin path can
          // serialize and the server's XML parser can read back (SingleFileStore is
          // not supported by the config parser, so it cannot round-trip).
          ccb.persistence()
             .addSoftIndexFileStore()
             .dataLocation("caching-store-" + name)
             .indexLocation("caching-store-" + name)
             .purgeOnStartup(true);
       }
      ccb.statistics().enable();

      RemoteCacheManagerAdmin admin = rcm.administration();
      try {
         admin.removeCache(name);
      } catch (Exception ignored) {
         // no cache of that name yet; nothing to remove
      }
      return admin.createCache(name, ccb.build());
   }

   private CacheFactory() {
   }
}
