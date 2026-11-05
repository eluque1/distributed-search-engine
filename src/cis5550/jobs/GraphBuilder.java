package cis5550.jobs;

import cis5550.flame.FlameContext;
import static cis5550.jobs.Crawler.normalizeURL;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.util.*;

public class GraphBuilder {
    private static final Logger logger = Logger.getLogger(GraphBuilder.class);

    public static void run(FlameContext ctx, String[] args) {
        ctx.output("Starting GraphBuilder (Underscore Key + Double Normalization)...\n");

        try {
            KVSClient kvs = ctx.getKVS();
            
            // Cleanup check
            if (kvs.scan("pt-graph").hasNext()) {
                ctx.output("WARNING: pt-graph exists. Please DELETE it before running this job.\n");
            } else {
                 Row dummyRow = new Row("__dummy__");
                 dummyRow.put("value", "1;1;0;"); 
                 kvs.putRow("pt-graph", dummyRow);
            }

            Iterator<Row> iter = kvs.scan("pt-crawl");
            
            int batchSize = 1000;
            int batchNum = 0;
            int totalProcessed = 0;

            while (iter.hasNext()) {
                int rowsCollected = 0;
                List<Row> batch = new ArrayList<>();
                while (rowsCollected < batchSize && iter.hasNext()) {
                    batch.add(iter.next());
                    rowsCollected++;
                }
                if (batch.isEmpty()) break;

                logger.info("Batch " + batchNum + " (" + batch.size() + " rows)");
                ctx.output("Processing Batch " + batchNum + "\n");

                for (Row row : batch) {
                    try {
                        String rawUrl = row.get("url");
                        String page = row.get("page");
                        if (rawUrl == null || page == null) continue;

                        // 1. SAFETY: Normalize Source URL against itself
                        // This prevents mismatch between 'raw' source and 'extracted' link
                        String selfNormalized = normalizeURL(rawUrl, rawUrl);
                        if (selfNormalized == null) continue;
                        
                        String rowKey = hashURLForDistribution(selfNormalized);
                        
                        // 2. Process Outlinks
                        List<String> rawLinks = Crawler.extractURLs(page);
                        Set<String> normalizedLinks = new LinkedHashSet<>(); 
                        
                        for (String href : rawLinks) {
                            String normalized = normalizeURL(rawUrl, href);
                            if (normalized != null && 
                                (normalized.startsWith("http://") || normalized.startsWith("https://"))) {
                                
                                String linkHash = hashURLForDistribution(normalized);
                                normalizedLinks.add(linkHash);
                            }
                        }

                        // 3. Build Row
                        int outdegree = normalizedLinks.size();
                        StringBuilder val = new StringBuilder();
                        val.append("1.0;1.0;"); 
                        val.append(outdegree).append(";");
                        val.append(String.join(",", normalizedLinks));
                        
                        Row graphRow = new Row(rowKey);
                        graphRow.put("value", val.toString());
                        kvs.putRow("pt-graph", graphRow);
                        
                        totalProcessed++;
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                batchNum++;
            }
            ctx.output("Done. Processed " + totalProcessed + " URLs.\n");

        } catch (Exception e) {
            e.printStackTrace();
            ctx.output("Error: " + e.getMessage());
        }
    }

    /**
     * GENERATES "safe" KEYS using UNDERSCORE
     * Format: hash(path+host)_hash(host+path)
     */
    private static String hashURLForDistribution(String url) {
        if (url == null) return null;
        try {
            String[] parts = URLParser.parseURL(url);
            if (parts == null || parts.length < 4) return Hasher.hash(url); 

            String host = parts[1];
            String port = parts[2];
            String path = parts[3];

            if (host == null) return Hasher.hash(url);

            host = host.toLowerCase();
            boolean isSecure = url.startsWith("https:");
            String canonicalHost = host;
            
            if (port != null && !port.isEmpty()) {
                if ((isSecure && "443".equals(port)) || (!isSecure && "80".equals(port))) {
                } else {
                    canonicalHost = host + ":" + port;
                }
            }
            String canonicalPath = (path == null || path.isEmpty()) ? "/" : path;
            
            String seed1 = canonicalPath + canonicalHost;
            String seed2 = canonicalHost + canonicalPath;
            
            // THE FIX: Use "_" instead of ":"
            return Hasher.hash(seed1) + "_" + Hasher.hash(seed2);

        } catch (Exception e) {
            return Hasher.hash(url);
        }
    }
}