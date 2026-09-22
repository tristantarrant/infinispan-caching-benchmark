package org.infinispan.bench.caching;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.configuration.NearCacheMode;

/** Builds a {@link RemoteCacheManager} from the parsed arguments. */
public final class ClientConfig {
   private static final int DEFAULT_NEAR_CACHE_SIZE = 100_000;

   public static RemoteCacheManager create(Args a) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      for (String hp : a.str("hosts", "").split(",")) {
         if (hp.isBlank()) continue;
         String[] parts = hp.split(":");
         cb.addServer().host(parts[0]).port(parts.length > 1 ? Integer.parseInt(parts[1]) : 11222);
      }

      cb.security().authentication()
         .username(a.str("username", "admin"))
         .password(a.str("password", "secret"))
         .realm("default")
         .saslMechanism(a.str("sasl-mechanism", "DIGEST-SHA-256"));

      // One active connection per worker thread so the near cache (and its bloom
      // filter) can be driven at full concurrency without blocking on the pool.
      int threads = Math.max(1, a.intVal("threads", 8));
      cb.connectionPool().maxActive(threads).exhaustedAction(ExhaustedAction.WAIT);

      if ("invalidated".equalsIgnoreCase(a.str("near-cache", "none"))) {
         int size = a.intVal("near-cache-size", DEFAULT_NEAR_CACHE_SIZE);
         if (size <= 0) size = DEFAULT_NEAR_CACHE_SIZE; // required when --bloom is set
         cb.remoteCache(a.str("cache", "cache"))
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(size)
            .nearCacheUseBloomFilter(a.bool("bloom"));
      }
      return new RemoteCacheManager(cb.build());
   }

   private ClientConfig() {
   }
}
