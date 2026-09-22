package org.infinispan.bench.caching;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal {@code --name value} argument parser for the workload driver.
 *
 * <p>A flag with no following value (or whose next token is itself a flag) is treated
 * as a boolean present-flag, e.g. {@code --bloom} or {@code --no-prefill}.
 */
public final class Args {
   private final Map<String, String> raw = new LinkedHashMap<>();

   public static Args parse(String[] argv) {
      Args a = new Args();
      for (int i = 0; i < argv.length; i++) {
         String token = argv[i];
         if (!token.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + token);
         String name = token.substring(2);
         String value = null;
         if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) value = argv[++i];
         a.raw.put(name, value != null ? value : "true");
      }
      return a;
   }

   public String str(String name, String def) {
      return raw.containsKey(name) ? raw.get(name) : def;
   }

   /** Presence means {@code true}. */
   public boolean bool(String name) {
      return raw.containsKey(name);
   }

   public int intVal(String name, int def) {
      return raw.containsKey(name) ? Integer.parseInt(raw.get(name)) : def;
   }

   public long lng(String name, long def) {
      return raw.containsKey(name) ? Long.parseLong(raw.get(name)) : def;
   }

   public double dbl(String name, double def) {
      return raw.containsKey(name) ? Double.parseDouble(raw.get(name)) : def;
   }

   public static void help(PrintStream out) {
      out.println("usage: java -jar caching-bench.jar --hosts host:port,host:port [options]");
      out.println();
      out.println("connection:");
      out.println("  --hosts <list>          comma-separated host:port list (required)");
      out.println("  --cache <name>          cache name (default: cache)");
      out.println("  --username <u>          default: admin");
      out.println("  --password <p>          default: secret");
      out.println("  --sasl-mechanism <m>    default: DIGEST-SHA-256");
      out.println();
      out.println("workload:");
      out.println("  --warmup <s>            seconds to discard before measuring (default: 10)");
      out.println("  --duration <s>          seconds to measure (default: 60)");
      out.println("  --keys <n>              working-set size in keys (default: 200000)");
      out.println("  --value-size <b>        value size in bytes (default: 1024)");
      out.println("  --read-ratio <0..1>     fraction of operations that are reads (default: 0.9)");
      out.println("  --distribution <d>      uniform | zipf (default: uniform)");
       out.println("  --threads <n>           client concurrency (default: 8)");
       out.println("  --no-prefill            skip the one-shot working-set population (default: prefill)");
       out.println("  --client-index <i>      this client's slice index (0-based, default: 0)");
       out.println("  --client-count <n>      number of slices; client i owns keys [i*keys/n, (i+1)*keys/n)");
       out.println("  --no-create-cache       attach to an existing cache instead of creating one");
       out.println("  --init-only             create the cache + prefill, then exit (no workload)");
       out.println();
      out.println("cache shape (created at runtime on the cluster via the Hot Rod admin API):");
      out.println("  --owners <n>            number of owners for the distributed cache (default: 2)");
      out.println("  --persistence           add a single-file store (stand-in for a system of record)");
      out.println();
      out.println("near cache (client-side):");
      out.println("  --near-cache <nc>       none | invalidated (default: none)");
      out.println("  --near-cache-size <n>   max near-cache entries (default: 100000)");
      out.println("  --bloom                 enable the near-cache bloom filter");
      out.println("  --db-latency-ms <ms>    simulated system-of-record load applied on a read miss");
      out.println();
      out.println("output (recorded in the JSON result):");
      out.println("  --step <n>              benchmark step");
      out.println("  --nodes <n>             cluster size");
      out.println("  --result-dir <dir>      write <run-name>.json here");
      out.println("  --run-name <name>       result file base name");
   }
}
