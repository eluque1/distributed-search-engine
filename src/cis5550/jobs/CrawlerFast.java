package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CrawlerFast {

    private static final Logger logger = Logger.getLogger(CrawlerFast.class);

    /** ----------- LOCAL CACHE (no remote calls) ----------- **/
    private static final Set<String> visited = ConcurrentHashMap.newKeySet();
    private static final Map<String,String> robotsCache = new ConcurrentHashMap<>();

    /** ----------- Speed tuning knobs ------------ **/
    private static final int FLUSH_BATCH = 200;   // flush KVS every 200 writes
    private static final int MAX_QUEUE_ITERATIONS = 5000;
    private static final int MAX_DISCOVERED = 1_000_000;

    public static void run(FlameContext ctx, String[] args) throws Exception {

        if (args.length != 1) {
            ctx.output("CrawlerFast requires 1 argument — seed URL\n");
            return;
        }

        String seed = normalizeURL(args[0], "");
        if (seed == null) {
            ctx.output("Invalid seed\n");
            return;
        }

        // initial frontier
        List<String> frontier = new ArrayList<>();
        frontier.add(seed);

        long totalFound = 0;
        int iteration = 0;

        ctx.output("Crawl starting at " + seed + "\n");

        String coordinator;
        try {
            coordinator = ctx.getKVS().getCoordinator();
        } catch (Exception e) {
            ctx.output("Failed to get KVS coordinator\n");
            return;
        }

        while (!frontier.isEmpty()
                && iteration < MAX_QUEUE_ITERATIONS
                && totalFound < MAX_DISCOVERED) {

            iteration++;
            ctx.output("Iteration " + iteration + " — frontier size=" + frontier.size() + "\n");

            FlameRDD rdd = ctx.parallelize(frontier);

            FlameRDD nextRDD = rdd.flatMap(url -> {

                List<String> out = new ArrayList<>();

                // cheap, local dedup
                String hash = Hasher.hash(url);
                if (!visited.add(hash)) {
                    return Collections.emptyList();
                }

                long start = System.currentTimeMillis();

                KVSClient client = null;
                try {
                    client = new KVSClient(coordinator);

                    // GET page directly — avoids HEAD round trip
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    conn.setInstanceFollowRedirects(false);
                    conn.setRequestProperty("User-Agent", "cis5550-crawler-fast");
                    conn.connect();

                    int code = conn.getResponseCode();
                    if (code >= 400) return Collections.emptyList();
                    if (!isHTML(conn)) return Collections.emptyList();

                    // read body
                    InputStream in = conn.getInputStream();
                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    byte[] tmp = new byte[4096];
                    int n;
                    while ((n = in.read(tmp)) != -1) buf.write(tmp, 0, n);

                    String body = buf.toString();

                    Row pageRow = new Row(hash);
                    pageRow.put("url", url);
                    pageRow.put("page", body);
                    pageRow.put("responseCode", String.valueOf(code));
                    pageRow.put("length", String.valueOf(body.length()));

                    for (String col : pageRow.columns()) {
                        client.putBatched("pt-crawl", pageRow.key(), col, pageRow.getBytes(col));
                    }

                    // extract links (fast)
                    for (String link : extractURLs(body)) {
                        String norm = normalizeURL(url, link);
                        if (norm != null && norm.startsWith("http")) {
                            out.add(norm);
                        }
                    }

                } catch (Exception e) {
                    return Collections.emptyList();
                } finally {
                    if (client != null) {
                        try { client.flush(); } catch(Exception ignored) {}
                    }
                }

                return out;
            });

            frontier = nextRDD.collect();
            totalFound += frontier.size();

            nextRDD.destroy();
            rdd.destroy();
        }

        ctx.output("Finished — total discovered=" + totalFound + "\n");
    }

    /* ========== SIMPLE FAST HELPERS BELOW — no remote calls ========== */

    private static boolean isHTML(HttpURLConnection conn) {
        String type = conn.getContentType();
        return type != null && type.toLowerCase().contains("text/html");
    }

    public static List<String> extractURLs(String html) {
        List<String> urls = new ArrayList<>();
        String lower = html.toLowerCase();
        int pos = 0;

        while (true) {
            int idx = lower.indexOf("href=", pos);
            if (idx == -1) break;
            int start = idx + 5;
            char quote = lower.charAt(start);
            if (quote == '"' || quote == '\'') {
                int end = lower.indexOf(quote, start+1);
                if (end > start) {
                    urls.add(html.substring(start+1, end));
                }
                pos = end + 1;
            } else {
                pos = start;
            }
        }
        return urls;
    }

    public static String normalizeURL(String base, String href) {
        try {
            if (href == null || href.isBlank()) return base;
            int hash = href.indexOf('#');
            if (hash != -1) href = href.substring(0, hash);
            URL absolute = new URL(new URL(base), href);
            return absolute.toString();
        } catch (Exception e) {
            return null;
        }
    }
}
 
