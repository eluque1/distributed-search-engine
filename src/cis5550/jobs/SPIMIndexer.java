package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Logger;
import java.util.*;

public class SPIMIndexer {
    private static final Logger logger = Logger.getLogger(SPIMIndexer.class);
    
    private static void safeDestroy(FlameRDD rdd, String name, FlameContext ctx) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                logger.error("Failed to destroy " + name, e);
            }
        }
    }

    private static void safeDestroy(FlamePairRDD rdd, String name, FlameContext ctx) {
        if (rdd != null) {
            try {
                rdd.destroy();
            } catch (Exception e) {
                logger.error("Failed to destroy " + name, e);
            }
        }
    }

    public static void run(FlameContext ctx, String[] args) {
        ctx.output("SPIMIndexer job started!\n");
        try {
            KVSClient kvs = ctx.getKVS();
            String kvsCoordinator = kvs.getCoordinator();
            
            // Phase 1: Batch Processing & Partial Indexes
            ctx.output("Scanning pt-crawl table...\n");
            Iterator<Row> iter = kvs.scan("pt-crawl");
            
            // Dynamic batch size (target ~50% heap)
            long maxMemory = Runtime.getRuntime().maxMemory();
            long targetBatchBytes = maxMemory / 2;
            int batchNum = 0;
            List<String> tempTables = new ArrayList<>();
            
            while (iter.hasNext()) {
                long currentBatchBytes = 0;
                List<String> batch = new ArrayList<>();
                
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String url = row.get("url");
                    String page = row.get("page");
                    
                    if (url != null && page != null) {
                        String entry = url + "," + page;
                        batch.add(entry);
                        currentBatchBytes += entry.length() * 2 + 40; // Approx memory usage
                    }
                    
                    if (currentBatchBytes >= targetBatchBytes) {
                        break;
                    }
                }
                
                if (batch.isEmpty()) break;
                
                batchNum++;
                ctx.output("Processing batch " + batchNum + " (~" + (currentBatchBytes / 1024 / 1024) + " MB)...\n");
                
                FlameRDD crawlRDD = null;
                FlamePairRDD wordUrlPairs = null;
                FlamePairRDD invertedBatch = null;
                
                try {
                    long start = System.currentTimeMillis();
                    crawlRDD = ctx.parallelize(batch);
                    
                    // 1. Invert: (URL, Page) -> (Word, URL)
                    // Performs tokenization, filtering, and mapping in one step
                    wordUrlPairs = crawlRDD.flatMapToPair(s -> {
                        int comma = s.indexOf(',');
                        if (comma == -1) return Collections.emptyList();
                        String url = s.substring(0, comma);
                        String page = s.substring(comma + 1);
                        
                        // Parse & Tokenize
                        String text = page.replaceAll("<[^>]*>", " ");
                        text = text.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
                        String[] words = text.split("\\s+");
                        
                        Set<String> uniqueWords = new HashSet<>();
                        for (String w : words) {
                            if (!w.isEmpty() && w.length() <= 200) {
                                uniqueWords.add(w);
                            }
                        }
                        
                        List<FlamePair> pairs = new ArrayList<>();
                        for (String w : uniqueWords) {
                            pairs.add(new FlamePair(w, url));
                        }
                        return pairs;
                    });
                    
                    // 2. Aggregate: (Word, URL) -> (Word, URL1,URL2...)
                    // This creates the posting lists for the current block
                    invertedBatch = wordUrlPairs.foldByKey("", (existing, newUrl) -> {
                        if (existing.isEmpty()) return newUrl;
                        return existing + "," + newUrl;
                    });
                    
                    // 3. Write Block to Disk (Persist as Temporary Table)
                    String tempTableName = "pt-index-temp-" + System.currentTimeMillis() + "-" + batchNum;
                    invertedBatch.saveAsTable(tempTableName);
                    tempTables.add(tempTableName);
                    
                    ctx.output("Saved partial index to " + tempTableName + " (" + (System.currentTimeMillis() - start) + "ms)\n");
                    
                } finally {
                    safeDestroy(crawlRDD, "crawlRDD", ctx);
                    safeDestroy(wordUrlPairs, "wordUrlPairs", ctx);
                    // invertedBatch is backed by the temp table, so we leave it be
                }
            }
            
            // Phase 2: Distributed Merge (SPIMI-Merge)
            // Combine all partial index blocks into the final inverted index
            ctx.output("Starting merge phase with " + tempTables.size() + " partial indexes...\n");
            
            if (!tempTables.isEmpty()) {
                FlameRDD tablesRDD = ctx.parallelize(tempTables);
                
                // Stream all partial indexes
                FlamePairRDD allData = tablesRDD.flatMapToPair(tableName -> {
                    KVSClient workerKVS = new KVSClient(kvsCoordinator);
                    Iterator<Row> rowIter = workerKVS.scan(tableName);
                    
                    return () -> new Iterator<FlamePair>() {
                        public boolean hasNext() {
                            boolean has = rowIter.hasNext();
                            if (!has) {
                                try { workerKVS.delete(tableName); } catch(Exception e) {}
                            }
                            return has;
                        }

                        public FlamePair next() {
                            Row r = rowIter.next();
                            String word = r.key();
                            String urls = "";
                            for (String col : r.columns()) {
                                urls = r.get(col);
                                break;
                            }
                            return new FlamePair(word, urls);
                        }
                    };
                });

                // Global Merge: Combine all partial URL lists into the final posting list
                FlamePairRDD finalIndex = allData.foldByKey("", (a, b) -> {
                    if (a.isEmpty()) return b;
                    if (b.isEmpty()) return a;
                    return a + "," + b;
                });
                
                // Write Final Index to Disk
                finalIndex.saveAsTable("pt-index");
                
                ctx.output("Final merge complete. Index saved to pt-index.\n");
            } else {
                ctx.output("No data found to index.\n");
            }
            
            ctx.output("SPIMIndexer job completed successfully!\n");
            
        } catch (Exception e) {
            logger.error("SPIMIndexer failed", e);
            ctx.output("ERROR: " + e.getMessage() + "\n");
            e.printStackTrace();
        }
    }
}
