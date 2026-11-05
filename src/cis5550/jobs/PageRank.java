package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import java.util.*;

public class PageRank {
    private static final Logger logger = Logger.getLogger(PageRank.class);

    public static void run(FlameContext ctx, String[] args) {
        if (args.length < 1) {
            ctx.output("Syntax: PageRank <threshold> [percentRequired] [fixedIterations]");
            return;
        }
        double threshold = Double.parseDouble(args[0]);
        double percentRequired = 100.0;
        if (args.length > 1) percentRequired = Double.parseDouble(args[1]);
        int fixedIterations = -1;
        if (args.length > 2) fixedIterations = Integer.parseInt(args[2]);

        KVSClient kvs = ctx.getKVS();
        String currentGraphTable = "pt-graph"; 
        int startIter = 0; 

        try {
            // STEP 0: VALIDATION CACHE
            ctx.output("Step 0: Loading valid URLs from " + currentGraphTable + "...\n");
            Set<String> validUrlSet = new HashSet<>();
            Iterator<Row> scanner = kvs.scan(currentGraphTable);
            while (scanner.hasNext()) {
                // TRIM HERE: Prevent invisible chars from breaking validation
                validUrlSet.add(scanner.next().key().trim());
            }
            ctx.output("Loaded " + validUrlSet.size() + " valid URLs. Starting Iteration " + (startIter+1) + "...\n");
            ctx.output("=== DEBUG: VALID URL SET ===\n");
            ctx.output("Valid URL set size: " + validUrlSet.size() + "\n");
            if (!validUrlSet.isEmpty()) {
                Iterator<String> sampleIter = validUrlSet.iterator();
                ctx.output("Sample key 1: [" + sampleIter.next() + "]\n");
                if (sampleIter.hasNext()) ctx.output("Sample key 2: [" + sampleIter.next() + "]\n");
            }
            ctx.output("==========================\n");
            if (startIter > 0) currentGraphTable = "pt-graph-iter-" + startIter;

            int i = startIter;
            boolean useFixedIterations = fixedIterations > 0;
            int targetIteration = useFixedIterations ? (startIter + fixedIterations) : Integer.MAX_VALUE;
            if (useFixedIterations) {
                ctx.output("Fixed-iteration mode enabled: running " + fixedIterations + " iteration(s).\n");
            }

            while (i < targetIteration) {
                i++;
                String nextGraphTable = "pt-graph-iter-" + i;
                String updatesTable = "pt-updates-iter-" + i;
                
                ctx.output("--- Iteration " + i + " ---\n");
                ctx.output("Writing to:   " + nextGraphTable + "\n");

                // ---------------------------------------------------------
                // PHASE 1: CALCULATE TRANSFERS
                // ---------------------------------------------------------
                Iterator<Row> iter = kvs.scan(currentGraphTable);
                Map<String, Double> updateBuffer = new HashMap<>();
                int processed = 0;
                
                while (iter.hasNext()) {
                    Row r = iter.next();
                    String val = r.get("value");
                    if (val == null) continue;
                    String[] parts = val.split(";", 4); 
                    if (parts.length < 4) continue;

                    double currentRank = Double.parseDouble(parts[0]);
                    int outDegree = Integer.parseInt(parts[2]);
                    String linksStr = parts[3];
                    if (outDegree > 0 && !linksStr.isEmpty()) {
                        double amountToGive = (0.9 * currentRank) / outDegree;
                        String[] links = linksStr.split(",");
                        
                        // ADD THESE COUNTERS
                        int linksChecked = 0;
                        int linksValid = 0;

                        for (String link : links) {
                            String cleanLink = link.trim();
                            linksChecked++;
                            
                            // DEBUG: Print first 3 links from first row
                            if (processed == 0 && linksChecked <= 3) {
                                ctx.output("DEBUG Link " + linksChecked + ": [" + cleanLink + "]\n");
                                ctx.output("  In validUrlSet? " + validUrlSet.contains(cleanLink) + "\n");
                            }
                            
                            if (validUrlSet.contains(cleanLink)) {
                                linksValid++;
                                updateBuffer.put(cleanLink, updateBuffer.getOrDefault(cleanLink, 0.0) + amountToGive);
                            }
                        }
                        
                        // Print stats after first batch
                        if (processed == 5000) {
                            ctx.output("LINK STATS: Checked=" + linksChecked + ", Valid=" + linksValid + "\n");
                        }
                    }
                    // if (outDegree > 0 && !linksStr.isEmpty()) {
                    //     double amountToGive = (0.85 * currentRank) / outDegree;
                    //     String[] links = linksStr.split(",");

                    //     for (String link : links) {
                    //         // TRIM LINK: Ensure we don't have spaces around commas
                    //         String cleanLink = link.trim();
                    //         if (validUrlSet.contains(cleanLink)) {
                    //             updateBuffer.put(cleanLink, updateBuffer.getOrDefault(cleanLink, 0.0) + amountToGive);
                    //         }
                    //     }
                    // }
                    processed++;
                    if (processed % 5000 == 0) {
                        flushUpdatesToKVS(kvs, updatesTable, updateBuffer);
                        updateBuffer.clear();
                        ctx.output("Processed " + processed + " rows (Phase 1)...\n");
                    }
                }
                flushUpdatesToKVS(kvs, updatesTable, updateBuffer);
                updateBuffer.clear();

                // ---------------------------------------------------------
                // PHASE 2: UPDATE RANKS (With Key Fix)
                // ---------------------------------------------------------
                iter = kvs.scan(currentGraphTable); 
                double maxDiff = 0.0;
                long totalPages = 0;
                long convergedCount = 0;
                
                // Debug Counter
                int debugSuccess = 0;
                
                while (iter.hasNext()) {
                    Row r = iter.next();
                    
                    // *** THE FIX: TRIM THE KEY ***
                    // If your KVS scan() leaves a newline at the end, this removes it.
                    String key = r.key().trim();
                    
                    String val = r.get("value");
                    String[] parts = val.split(";", 4);
                    double oldRank = Double.parseDouble(parts[0]);
                    
                    // Lookup using the CLEAN key
                    Row updateRow = kvs.getRow(updatesTable, key);
                    
                    double sumIncoming = 0.0;
                    if (updateRow != null && updateRow.get("acc") != null) {
                        sumIncoming = Double.parseDouble(updateRow.get("acc"));
                        
                        // DEBUG PROOF: Print first few successes
                        if (debugSuccess < 3) {
                            ctx.output("DEBUG SUCCESS: Found update for [" + key + "] -> " + sumIncoming + "\n");
                            debugSuccess++;
                        }
                    }
                    
                    double newRank = 0.15 + sumIncoming;
                    double diff = Math.abs(newRank - oldRank);
                    if (diff < threshold) convergedCount++;
                    if (diff > maxDiff) maxDiff = diff;
                    totalPages++;
                    
                    Row newRow = new Row(key);
                    newRow.put("value", newRank + ";" + oldRank + ";" + parts[2] + ";" + parts[3]);
                    kvs.putRow(nextGraphTable, newRow);
                }
                
                double percentConv = (totalPages == 0) ? 0 : (100.0 * convergedCount / totalPages);
                ctx.output("Iter " + i + " Stats: MaxDiff=" + maxDiff + ", Conv=" + percentConv + "%\n");
                
                currentGraphTable = nextGraphTable;

                if (!useFixedIterations && (maxDiff < threshold || percentConv >= percentRequired)) {
                    break;
                }
            }

            // FINAL EXPORT
            if (useFixedIterations) {
                ctx.output("Fixed iteration limit reached. Saving pt-pageranks...\n");
            } else {
                ctx.output("Convergence reached! Saving pt-pageranks...\n");
            }
            Iterator<Row> finalIter = kvs.scan(currentGraphTable);
            while (finalIter.hasNext()) {
                Row r = finalIter.next();
                String key = r.key().trim(); // TRIM HERE TOO
                String[] parts = r.get("value").split(";");
                Row prRow = new Row(key);
                prRow.put("rank", parts[0]);
                kvs.putRow("pt-pageranks", prRow);
            }
            ctx.output("DONE.\n");

        } catch (Exception e) {
            e.printStackTrace();
            ctx.output("CRASHED: " + e.getMessage() + "\n");
        }
    }

    private static void flushUpdatesToKVS(KVSClient kvs, String tableName, Map<String, Double> buffer) throws Exception {
        for (Map.Entry<String, Double> entry : buffer.entrySet()) {
            String url = entry.getKey();
            double valueToAdd = entry.getValue();
            Row r = kvs.getRow(tableName, url);
            double currentSum = 0.0;
            if (r != null && r.get("acc") != null) {
                currentSum = Double.parseDouble(r.get("acc"));
            }
            Row newRow = new Row(url);
            newRow.put("acc", String.valueOf(currentSum + valueToAdd));
            kvs.putRow(tableName, newRow);
        }
    }
}