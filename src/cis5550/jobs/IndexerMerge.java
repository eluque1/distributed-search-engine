package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;

public class IndexerMerge {
    private static final Logger logger = Logger.getLogger(IndexerMerge.class);
    /*
    * Safely destroy an RDD.
    * @param rdd The RDD to destroy.
    * @param name The name of the RDD.
    */
    private static void safeDestroy(FlameRDD rdd, String name) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                logger.error("Error destroying " + name + " RDD: " + e.getMessage(), e);
            }
            logger.info("Destroyed " + name + " RDD");
        }
    }

    private static void safeDestroy(FlamePairRDD rdd, String name) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                logger.error("Error destroying " + name + " RDD: " + e.getMessage(), e);
            }
            logger.info("Destroyed " + name + " RDD");
        }
    }
    /*TODO: instead of merging two tables and adding it back 
    to queue, save it in memory and merge the disc tables with
     the in memeory one which we save as pt-index at the end
     this will reduce time for writting in memory RDDs and also we should avoid
     renaming in memeory tables to disk tables
    * Merge partial index tables into a single index table.
    * @param ctx The Flame context.
    * @param args The command line arguments.
    */

    public static void run(FlameContext ctx, String[] args) {
        try {

            KVSClient kvs = ctx.getKVS();
        
            List<String> partialTables = kvs.listTables()
                .stream()
                .filter(t -> t.contains("pt-pip"))
                .collect(Collectors.toList());
        
            if (partialTables.isEmpty()) {
                logger.info("No partial index tables found to merge.");
                return;
            }
            
            logger.info("Found " + partialTables.size() + " partial index tables to merge.");
            logger.info("Loading and combining all postings...");

            // Initialize accumulator if not exists
            if (!kvs.listTables().contains("pt-index")) {
                // Create empty pt-index table by putting a dummy row
                Row dummyRow = new Row("__dummy__");
                dummyRow.put("value", "");
                kvs.putRow("pt-index", dummyRow);
                logger.info("Created empty pt-index table");
            }
            
        
            for (String partialTable : partialTables) {
                logger.info("Merging " + partialTable + " into pt-index using range-based parallel merge...");
                //String[] rangeStarts = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", 
                //"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
                String[] rangeStarts = {"a", "h", "m", "s", "z"};

                
                for (int i = 0; i < rangeStarts.length; i++) {
                    String startKey = rangeStarts[i];
                    String endKey = (i < rangeStarts.length - 1) ? rangeStarts[i + 1] : null;
                    logger.info("Processing range: " + startKey + " to " + (endKey != null ? endKey : "end"));
                    
                    // Scan range from pt-index (accumulator)
                    List<String> accumulatorRange = new ArrayList<>();
                    try {
                        Iterator<Row> accIter = kvs.scan("pt-index", startKey, endKey);
                        while (accIter.hasNext()) {
                            Row row = accIter.next();
                            accumulatorRange.add(row.key() + "|" + row.get("value"));
                        }
                    } catch (Exception e) {
                        // Range might be empty, that's OK
                    }
                    
                    // Scan range from partial table
                    List<String> partialRange = new ArrayList<>();
                    try {
                        Iterator<Row> partialIter = kvs.scan(partialTable, startKey, endKey);
                        while (partialIter.hasNext()) {
                            Row row = partialIter.next();
                            partialRange.add(row.key() + "|" + row.get("value"));
                        }
                    } catch (Exception e) {
                        // Range might be empty, that's OK
                    }
                    
                    // Skip if both ranges are empty
                    if (accumulatorRange.isEmpty() && partialRange.isEmpty()) {
                        continue;
                    }
                    
                    // Convert scanned ranges to RDDs for parallel processing
                    FlameRDD accRDD = accumulatorRange.isEmpty() ? null : ctx.parallelize(accumulatorRange);
                    FlameRDD partialRDD = partialRange.isEmpty() ? null : ctx.parallelize(partialRange);
                    
                    // Convert to PairRDDs (term -> postings)
                    FlamePairRDD accPairRDD = null;
                    FlamePairRDD partialPairRDD = null;
                    
                    if (accRDD != null) {
                        accPairRDD = accRDD.mapToPair(row -> {
                            String[] parts = row.split("\\|", 2);
                            return new FlamePair(parts[0], parts.length > 1 ? parts[1] : "");
                        });
                    }
                    
                    if (partialRDD != null) {
                        partialPairRDD = partialRDD.mapToPair(row -> {
                            String[] parts = row.split("\\|", 2);
                            return new FlamePair(parts[0], parts.length > 1 ? parts[1] : "");
                        });
                    }
                    
                    // Merge using cogroupJoin if both exist, otherwise use the non-null one
                    FlamePairRDD mergedPairRDD;
                    if (accPairRDD != null && partialPairRDD != null) {
                        // cogroupJoin combines values from both RDDs with commas
                        // Since each value is already a comma-separated list, and always unique just cogroupJoin
                        mergedPairRDD = accPairRDD.cogroupJoin(partialPairRDD);
                        
                        // Clean up intermediate RDDs (cogroupJoin creates a new table, so safe to destroy sources)
                        safeDestroy(accRDD, "accRDD");
                        safeDestroy(partialRDD, "partialRDD");
                        safeDestroy(accPairRDD, "accPairRDD");
                        safeDestroy(partialPairRDD, "partialPairRDD");
                    } else if (accPairRDD != null) {
                        mergedPairRDD = accPairRDD;
                        // Don't destroy yet - we need to use it for saveAsTable
                        safeDestroy(accRDD, "accRDD");
                    } else if (partialPairRDD != null) {
                        mergedPairRDD = partialPairRDD;
                        // Don't destroy yet - we need to use it for saveAsTable
                        safeDestroy(partialRDD, "partialRDD");
                    } else {
                        continue; // Both empty, skip
                    }

                    // Save merged range to temporary table (distributed, happens on workers)
                    // Short, unique temp name to avoid partition collisions and path-length issues
                    String tempRangeTable = "tr-" + startKey + "-" + (System.currentTimeMillis() % 1_000_000) +
                                            "-" + (int)(Math.random() * 1_0000);
                    // Ensure no stale table blocks rename
                    try {
                        kvs.delete(tempRangeTable);
                    } catch (Exception ignore) {}
                    // Avoid collect() to prevent driver OOM
                    mergedPairRDD.saveAsTable(tempRangeTable);

                    
                    logger.info("For range " + startKey + " to " + (endKey != null ? endKey : "end") + 
                               " and partial table " + partialTable + " writing to pt-index");
                    
                    // Scan temp table and write directly to pt-index
                    try {
                        logger.info("Scanning temp table: " + tempRangeTable);
                        Iterator<Row> mergedIter = kvs.scan(tempRangeTable);
                        while (mergedIter.hasNext()) {
                            Row row = mergedIter.next();
                            // The row key is the term, and value column contains the postings
                            // cogroupJoin stores combined values in "value" column
                            String postingsValue = row.get("value");
                            if (postingsValue == null) {
                                // If "value" column doesn't exist, collect all column values
                                // This can happen if mapToPair created columns with UUID names
                                StringBuilder combined = new StringBuilder();
                                boolean first = true;
                                for (String col : row.columns()) {
                                    String val = row.get(col);
                                    if (val != null) {
                                        if (!first) combined.append(",");
                                        combined.append(val);
                                        first = false;
                                    }
                                }
                                postingsValue = combined.toString();
                            }
                            
                            if (postingsValue != null && !postingsValue.isEmpty()) {
                                Row ptIndexRow = new Row(row.key());
                                ptIndexRow.put("value", postingsValue);
                                kvs.putRow("pt-index", ptIndexRow);
                            }
                        }
                        // Clean up temporary table
                        kvs.delete(tempRangeTable);
                    } catch (Exception e) {
                        logger.warn("Could not process merged range table: " + e.getMessage(), e);
                    }
                    
                    // Destroy the merged RDD after we're done using the temp table
                    // Note: saveAsTable renamed the table, so destroying here is safe (table already deleted)
                    safeDestroy(mergedPairRDD, "mergedPairRDD");
                }

                // delete the partial table
                logger.info("Completed merging " + partialTable + " into pt-index");
                try {
                    kvs.delete(partialTable);
                    logger.info("Deleted " + partialTable + " table");
                } catch (Exception e) {
                    logger.error("Error deleting " + partialTable + " table: " + e.getMessage(), e);
                }
                
            }
            
            logger.info("SUCCESS: All partial tables merged into pt-index");
            
        } catch (Exception e) {
            logger.error("ERROR: " + e.getMessage(), e);
            e.printStackTrace();
        }
    }
}            
            

    
    /**
     * Normalizes postings list by deduplicating entries that represent the same URL
     * but were hashed differently (path+host vs host+path).
     * Postings format: "docKey1:tf1,docKey2:tf2,..."
     * Returns normalized postings with canonical hash keys.
     */
    /* 
    private static String normalizePostings(String postings, KVSClient kvs) {
        if (postings == null || postings.isEmpty()) return postings;
        
        // Parse postings into docKey -> tf map
        Map<String, Double> docKeyToTf = new HashMap<>();
        String[] entries = postings.split(",");
        for (String entry : entries) {
            if (entry.isEmpty()) continue;
            String[] parts = entry.split(":", 2);
            if (parts.length == 2) {
                try {
                    String docKey = parts[0];
                    double tf = Double.parseDouble(parts[1]);
                    docKeyToTf.put(docKey, docKeyToTf.getOrDefault(docKey, 0.0) + tf);
                } catch (NumberFormatException e) {
                    // Skip malformed entries
                    continue;
                }
            }
        }
        
        // Group by canonical hash (path+host) - look up URLs and normalize
        Map<String, Double> canonicalToTf = new HashMap<>();
        for (Map.Entry<String, Double> entry : docKeyToTf.entrySet()) {
            String docKey = entry.getKey();
            double tf = entry.getValue();
            
            // Try to get URL from pt-crawl to compute canonical hash
            // Try both hash variants since pt-crawl may have entries with different hash orders
            String canonicalKey = docKey; // Default to original if lookup fails
            try {
                Row crawlRow = kvs.getRow("pt-crawl", docKey);
                if (crawlRow == null && docKey.contains(":")) {
                    // If docKey is combined format (hash1:hash2), try individual hashes
                    String[] parts = docKey.split(":", 2);
                    if (parts.length == 2) {
                        crawlRow = kvs.getRow("pt-crawl", parts[0]);
                        if (crawlRow == null) {
                            crawlRow = kvs.getRow("pt-crawl", parts[1]);
                        }
                    }
                }
                
                if (crawlRow != null) {
                    String url = crawlRow.get("url");
                    if (url != null && !url.isEmpty()) {
                        // Compute canonical hash (path+host)
                        canonicalKey = hashURLCanonical(url);
                    }
                }
            } catch (Exception e) {
                // If lookup fails, keep original key
                logger.debug("Could not look up docKey " + docKey + " in pt-crawl: " + e.getMessage());
            }
            
            // Combine TFs for same canonical key
            canonicalToTf.put(canonicalKey, canonicalToTf.getOrDefault(canonicalKey, 0.0) + tf);
        }
        
        // Rebuild postings list with canonical keys
        List<String> normalizedEntries = new ArrayList<>();
        for (Map.Entry<String, Double> entry : canonicalToTf.entrySet()) {
            normalizedEntries.add(entry.getKey() + ":" + entry.getValue());
        }
        
        return String.join(",", normalizedEntries);
    }*/
    
    /**
     * Hash URL using canonical path+host convention (consistent with Crawler's hashURLForDistribution).
     */
    /* 
    private static String hashURLCanonical(String url) {
        try {
            String[] parts = URLParser.parseURL(url);
            if (parts == null || parts.length < 4) return Hasher.hash(url);
            
            String host = parts[1];
            String path = parts[3];
            
            if (host == null) return Hasher.hash(url);
            
            // Canonical: path+host (consistent with Crawler)
            String toHash = (path != null ? path : "/") + host;
            return Hasher.hash(toHash);
        } catch (Exception e) {
            return Hasher.hash(url);
        }
    }*/
        
            
