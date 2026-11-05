package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
// TODO: use gap binary compression, the list of ordered urls based on doc key and gaps calculated

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);
    // Maximum length for postings list value (500KB to prevent KVS write failures)
    private static final int MAX_POSTINGS_LENGTH = 512000;
    

    private static String safeDecode(String s) {
        if (s == null) return s;
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }
    
    /**
     * Decodes URL-encoded strings multiple times to handle double/triple encoding.
     * For example: %2527 -> %27 -> '
     */
    private static String deepDecode(String s) {
        if (s == null || !s.contains("%")) return s;
        
        String decoded = s;
        String previous = "";
        int maxIterations = 10; // Prevent infinite loops
        int iterations = 0;
        
        // Keep decoding until no more changes occur
        while (!decoded.equals(previous) && iterations < maxIterations) {
            previous = decoded;
            decoded = safeDecode(decoded);
            iterations++;
        }
        
        return decoded;
    }
    
    /**
     * Cleans and normalizes a term by decoding URL encoding and removing problematic characters.
     */
    private static String cleanTerm(String term) {
        if (term == null || term.isEmpty()) return term;
        
        // First, decode any URL encoding (handles double/triple encoding)
        term = deepDecode(term);
        
        // Remove any remaining URL-encoded patterns that couldn't be decoded
        if (term.contains("%")) {
            // If it still contains %, it's likely malformed - skip it
            return null;
        }
        
        // Trim whitespace
        term = term.trim();
        
        // Remove any control characters
        term = term.replaceAll("[\\p{Cntrl}]", "");
        
        return term;
    }

    private static String removeTagBlock(String html, String tag) {
        String lower = html.toLowerCase();
        String open = "<" + tag;
        String close = "</" + tag + ">";
        int start, end;

        while ((start = lower.indexOf(open)) != -1 &&
            (end = lower.indexOf(close, start)) != -1) {
            html = html.substring(0, start) + " " + html.substring(end + close.length());
            lower = html.toLowerCase();
        }
        return html;
    }

    private static String sanitizeText(String page) {

        // Remove JS, CSS, SVG, noisy blocks
        page = removeTagBlock(page, "script");
        page = removeTagBlock(page, "style");
        page = removeTagBlock(page, "svg");
    
        // Strip tags only (you already do this, but we’ll reuse your method)
        StringBuilder text = new StringBuilder();
        boolean inTag = false;
        for (int i = 0; i < page.length(); i++) {
            char c = page.charAt(i);
            if (c == '<') { inTag = true; text.append(' '); }
            else if (c == '>') { inTag = false; text.append(' '); }
            else if (!inTag) text.append(c);
        }
    
        String s = text.toString();
    
        // Decode URL-encoded pieces: %22hello%2C → "hello,
        s = safeDecode(s);
    
        // Decode uXXXX unicode escapes
        Pattern unicodePattern = Pattern.compile("\\\\u([0-9A-Fa-f]{4})");
        Matcher matcher = unicodePattern.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            int code = Integer.parseInt(matcher.group(1), 16);
            matcher.appendReplacement(sb, Character.toString((char) code));
        }
        matcher.appendTail(sb);
        s = sb.toString();
    
        // Normalize accents (café → cafe)
        s = Normalizer.normalize(s, Normalizer.Form.NFD)
                      .replaceAll("\\p{M}", "");
    
        // Replace punctuation with space
        s = s.replaceAll("[^a-zA-Z0-9'-]", " ");
    
        return s.toLowerCase();
    }
    


    private static String stem(String word) {
        if (word == null || word.isEmpty()) return word;
        
        // Convert to lowercase (PorterStemmer expects lowercase)
        word = word.toLowerCase(Locale.ROOT);
        
        // For very short words, don't stem (PorterStemmer may not handle them well)
        if (word.length() <= 2) return word;
        
        // Use PorterStemmer for proper stemming
        PorterStemmer stemmer = new PorterStemmer();
        char[] chars = word.toCharArray();
        stemmer.add(chars, chars.length);
        stemmer.stem();
        return stemmer.toString();
    }

    private static Set<String> stopwords = Set.of(
        "the","a","an","and","or","but","if","of","to","in","on","for",
        "with","is","was","were","be","been","being","at","by","from",
        "that","this","it","as","are","not","so","such","their","there",
        "these","those","then","than"
    );
    
    // Check if a term is valid for indexing (not URL-encoded, code, etc.)
    private static boolean isValidTerm(String term) {
        if (term == null || term.isEmpty()) {
            return false;
        }
        
        // Filter out URL-encoded strings (start with %)
        if (term.startsWith("%")) {
            return false;
        }
        
        // Filter out very long terms (KVS filename limits - max 255 chars on Windows)
        if (term.length() > 200) {
            return false;
        }
        
        // Filter out strings that are mostly non-alphabetic (likely code/data)
        int alphaCount = 0;
        for (char c : term.toCharArray()) {
            if (Character.isLetter(c)) {
                alphaCount++;
            }
        }
        
        // Skip if less than 50% alphabetic (likely code/data, not text)
        if (alphaCount * 2 < term.length()) {
            return false;
        }
        
        // Filter out common code patterns and URL-encoded strings
        if (term.contains("://") || term.contains("function") || term.contains("var(") || 
            term.contains("url(") || term.contains("data:") || term.contains("javascript:") ||
            term.contains("style%") || term.contains("%") || term.contains("\\u") ||
            term.contains("&#") || term.contains("&amp;") || term.contains("&lt;") || 
            term.contains("&gt;") || term.contains("&quot;")) {
            return false;
        }
        
        // Filter out strings with too many special characters
        int specialCount = 0;
        for (char c : term.toCharArray()) {
            if (!Character.isLetterOrDigit(c) && c != '-' && c != '_' && c != '\'') {
                specialCount++;
            }
        }
        if (specialCount > term.length() / 3) {
            return false;
        }
        
        return true;
    }
    
    // Optimized tokenization - counts ALL occurrences, not just unique
    private static List<FlamePair> tokenizeDocument(String docId, String page, String kvsCoordinator) {
        List<FlamePair> pairs = new ArrayList<>();
    
        // NEW: clean the page fully
        String clean = sanitizeText(page);
    
        // Tokenize
        String[] words = clean.split("\\s+");
    
        Map<String, Integer> termCounts = new HashMap<>();
        int totalTerms = 0;
    
        for (String w : words) {
            if (w.length() == 0) continue;
            
            // Clean the word first (decode URL encoding, remove problematic chars)
            String cleaned = cleanTerm(w);
            if (cleaned == null || cleaned.isEmpty()) continue;

            double combinedTf = getCombinedTfForWord(cleaned, kvsCoordinator);
            // Try stemmed key too if the raw token has no accumulated TF yet.
            if (combinedTf <= 0.0) {
                String candidateStem = stem(cleaned);
                if (candidateStem != null && !candidateStem.isEmpty()) {
                    combinedTf = getCombinedTfForWord(candidateStem, kvsCoordinator);
                }
            }

    
            if (!stopwords.contains(cleaned) && isValidTerm(cleaned)) {
                String stemmed = stem(cleaned);
                if (stemmed != null && !stemmed.isEmpty() && isValidTerm(stemmed)) {
                    termCounts.put(stemmed, termCounts.getOrDefault(stemmed, 0) + 1);
                    totalTerms++;
                }
            }
        }
        
        // Emit (word, docId tf) pairs - value-to-key conversion
        // Calculate normalized TF: count / totalTerms
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            String term = entry.getKey();
            
            // Final validation: ensure term is safe for KVS row key
            // Check length again (KeyEncoder might add overhead)
            if (term == null || term.length() == 0 || term.length() > 200) {
                continue; // Skip invalid terms
            }
            
            // Check for characters that might cause issues in filenames
            // Windows doesn't allow: < > : " / \ | ? *
            if (term.contains("<") || term.contains(">") || term.contains(":") || 
                term.contains("\"") || term.contains("/") || term.contains("\\") ||
                term.contains("|") || term.contains("?") || term.contains("*")) {
                continue; // Skip terms with invalid filename characters
            }
            
            int count = entry.getValue();   
            double tf = totalTerms > 0 ? (double) count / totalTerms : 0.0;
            String tfStr = String.format("%.6f", tf);
            
            // Validate docId and tfStr are not null/empty
            if (docId == null || docId.isEmpty() || tfStr == null) {
                continue;
            }
            
            String value = docId + ":" + tfStr;
            // Ensure value isn't too long (though this is less critical than key)
            if (value.length() > 1000) {
                continue; // Skip if value is suspiciously long
            }
            
            pairs.add(new FlamePair(term, value));
        }
        
        return pairs;
    }

    private static double parseTfFromEntry(String postingEntry) {
        if (postingEntry == null || postingEntry.isEmpty()) {
            return 0.0;
        }
        int tfSeparator = postingEntry.lastIndexOf(':');
        if (tfSeparator < 0 || tfSeparator >= postingEntry.length() - 1) {
            return 0.0;
        }
        String tfPart = postingEntry.substring(tfSeparator + 1).trim();
        try {
            return Double.parseDouble(tfPart);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static double sumTfFromPostings(String postings) {
        if (postings == null || postings.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        String[] entries = postings.split(",");
        for (String entry : entries) {
            sum += parseTfFromEntry(entry);
        }
        return sum;
    }

    private static double scanWordTfInTable(KVSClient kvs, String tableName, String term) {
        double combined = 0.0;
        try {
            // Per-word KVS scan: [term, term + '\0') captures exactly this row key.
            Iterator<Row> rowIter = kvs.scan(tableName, term, term + '\0');
            while (rowIter.hasNext()) {
                Row row = rowIter.next();
                if (!term.equals(row.key())) {
                    continue;
                }

                String postings = row.get("value");
                if (postings == null || postings.isEmpty()) {
                    StringBuilder fallback = new StringBuilder();
                    boolean first = true;
                    for (String col : row.columns()) {
                        String colVal = row.get(col);
                        if (colVal == null || colVal.isEmpty()) {
                            continue;
                        }
                        if (!first) {
                            fallback.append(",");
                        }
                        fallback.append(colVal);
                        first = false;
                    }
                    postings = fallback.toString();
                }
                combined += sumTfFromPostings(postings);
            }
        } catch (Exception e) {
            logger.warn("Failed per-word scan for term '" + term + "' in table " + tableName + ": " + e.getMessage());
        }
        return combined;
    }

    private static double getCombinedTfForWord(String term, String kvsCoordinator) {
        if (term == null || term.isEmpty() || kvsCoordinator == null || kvsCoordinator.isEmpty()) {
            return 0.0;
        }

        double combinedTf = 0.0;
        try {
            KVSClient kvs = new KVSClient(kvsCoordinator);
            List<String> tables = kvs.listTables();
            for (String table : tables) {
                if ("pt-index".equals(table) || table.startsWith("pt-pip-")) {
                    combinedTf += scanWordTfInTable(kvs, table, term);
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to compute combined TF for term '" + term + "': " + e.getMessage());
        }
        return combinedTf;
    }
    
    private static void safeDestroy(FlameRDD rdd, String name, FlameContext ctx) {
        if (rdd != null) {
            try {
                // Get table name before destroying (if accessible)
                String tableName = "unknown";
                try {
                    tableName = ((FlameRDDImpl) rdd).getTableName();
                } catch (Exception e) {
                    // Can't access table name, that's OK
                }
                logger.info("Attempting to destroy " + name + " (table: " + tableName + ")");
                rdd.destroy();
                logger.info("Successfully called destroy() on " + name + " (table: " + tableName + ")");
            } catch (Exception e) {
                logger.error("Failed to destroy " + name + ": " + e.getMessage(), e);
            }
        } else {
            logger.debug(name + " is null, skipping destroy");
        }
    }
    
    private static void safeDestroy(FlamePairRDD rdd, String name, FlameContext ctx) {
        if (rdd != null) {
            try {
                logger.info("Attempting to destroy " + name);
                rdd.destroy();
                logger.info("Successfully called destroy() on " + name);
            } catch (Exception e) {
                logger.error("Failed to destroy " + name + ": " + e.getMessage(), e);
            }
        } else {
            logger.debug(name + " is null, skipping destroy");
        }
    }

    public static void run(FlameContext ctx, String[] args) {
        ctx.output("Indexer job started!\n");
        logger.info("Indexer job started!");
        try {
            KVSClient kvs = ctx.getKVS();
            final String kvsCoordinator = kvs.getCoordinator();
            ctx.output("Got KVS client\n");
            logger.info("Got KVS client");
            
            // Create pt-index table first if it doesn't exist
            boolean ptIndexExists = false;
            try {
                kvs.scan("pt-index").hasNext(); // Check if table exists
                ptIndexExists = true;
                ctx.output("pt-index table already exists\n");
            } catch (Exception e) {
                ptIndexExists = false;
                ctx.output("pt-index table does not exist, creating empty table\n");
            }
            
            if (!ptIndexExists) {
                // Create empty pt-index table by putting a dummy row
                // This ensures pt-index exists for subsequent batch updates
                Row dummyRow = new Row("__dummy__");
                dummyRow.put("value", "");
                kvs.putRow("pt-index", dummyRow);
                ctx.output("Created empty pt-index table\n");
            }
            

            ctx.output("Scanning pt-crawl table...\n");
            logger.info("Scanning pt-crawl table...");
            int timeStart = (int) System.currentTimeMillis();
            
            // Check if pt-crawl table exists and count rows
            List<String> tables = kvs.listTables();
            ctx.output("Available tables: " + tables + "\n");
            logger.info("Available tables: " + tables);
            
            if (!tables.contains("pt-crawl")) {
                ctx.output("ERROR: pt-crawl table does not exist!\n");
                logger.error("pt-crawl table does not exist!");
                return;
            }
            logger.info("Scanning pt-crawl table...");
            Iterator<Row> iter = kvs.scan("pt-crawl");
            ctx.output("Got iterator for pt-crawl\n");
            logger.info("Got iterator for pt-crawl");
            int timeEnd = (int) System.currentTimeMillis();
            logger.info("Time taken to get iterator: " + (timeEnd - timeStart) + "ms");
            // Check if iterator has any rows
            if (!iter.hasNext()) {
                ctx.output("WARNING: pt-crawl table exists but is empty (no rows found)!\n");
                logger.warn("pt-crawl table exists but is empty (no rows found)!");
                return;
            }
            
            ctx.output("pt-crawl table has data, starting to process...\n");
            logger.info("pt-crawl table has data, starting to process...");
            
            // Increased batch size for better performance (reduces overhead)
            int batchSize = 2000;
            int batchNum = 0;
            
            while (iter.hasNext()) {
                int rowsCollected = 0;
                List<String> batch = new ArrayList<>();
                while (rowsCollected < batchSize && iter.hasNext()) {
                    Row row = iter.next();
                    batch.add(row.key() + "," + row.get("url") + "," + row.get("page"));
                    rowsCollected++;
                }
                if (batch.isEmpty()) {
                    break;
                }
                
                // Initialize all RDDs to null for proper cleanup
                FlameRDD crawlRDD = null;
                FlamePairRDD urlPagePairs = null;
                FlamePairRDD wordUrlPairs = null;
                FlamePairRDD invertedBatch = null;
                
                try {
                    logger.info("Processing batch " + batchNum + " with " + batch.size() + " rows");
                    crawlRDD = ctx.parallelize(batch);
                    logger.info("Successfully created RDD for batch " + batchNum);

                    // Convert directly to pairs (removed unnecessary flatMap step)
                    urlPagePairs = crawlRDD.mapToPair(s -> {
                        int comma = s.indexOf(',');
                        if (comma == -1) return null;
                        String docKey = s.substring(0, comma);
                        int secondComma = s.indexOf(',', comma + 1);
                        if (secondComma == -1) return null;
                        String url = s.substring(comma + 1, secondComma);
                        String page = s.substring(secondComma + 1);
                        return new FlamePair(docKey, url + ":" + page);
                    });
                    safeDestroy(crawlRDD, "crawlRDD", ctx);
                    crawlRDD = null;

                    // Extract words and create (word, [url:tf]) pairs
                    // Filter out very long words early to avoid filename issues
                    logger.info("Calling flatMapToPair to tokenize documents for batch " + batchNum);
                    long flatMapStartTime = System.currentTimeMillis();
                    long flatMapTime = 0;
                    try {
                        wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
                            try {
                                String url = pair._1();
                                String page = pair._2();
                                
                                // Validate inputs
                                if (url == null || url.isEmpty() || page == null) {
                                    logger.warn("Skipping pair with null/empty url or page");
                                    return Collections.emptyList();
                                }
                                
                                // Check if URL is too long (might cause issues)
                                if (url.length() > 500) {
                                    logger.warn("Skipping document with very long URL: " + url.substring(0, 50) + "...");
                                    return Collections.emptyList();
                                }
                                
                                return tokenizeDocument(url, page, kvsCoordinator);
                            } catch (Exception e) {
                                logger.error("Error in tokenizeDocument for URL: " + (pair._1() != null ? pair._1().substring(0, Math.min(50, pair._1().length())) : "null"), e);
                                return Collections.emptyList(); // Return empty list instead of crashing
                            }
                        });
                        flatMapTime = System.currentTimeMillis() - flatMapStartTime;
                        logger.info("flatMapToPair completed for batch " + batchNum + " (took " + flatMapTime + "ms)");
                        ctx.output("flatMapToPair() took " + flatMapTime + "ms\n");
                    } catch (Exception e) {
                        flatMapTime = System.currentTimeMillis() - flatMapStartTime;
                        logger.error("ERROR during flatMapToPair for batch " + batchNum + ": " + e.getMessage() + " (took " + flatMapTime + "ms)", e);
                        throw new RuntimeException("flatMapToPair failed for batch " + batchNum + ": " + e.getMessage(), e);
                    }
                    safeDestroy(urlPagePairs, "urlPagePairs", ctx);
                    urlPagePairs = null;

                    // Fold by key: combine URLs for each word
                    // Cap the length to prevent KVS write failures
                    
                    logger.info("About to call foldByKey on wordUrlPairs for batch " + batchNum);
                    if (wordUrlPairs == null) {
                        logger.error("wordUrlPairs is null before foldByKey!");
                        throw new RuntimeException("wordUrlPairs is null");
                    }
                    
                    long foldStartTime = System.currentTimeMillis();
                    long foldTime = 0;
                    try {
                        logger.info("Calling foldByKey: combining URLs for each word");
                        invertedBatch = wordUrlPairs.foldByKey("", (existing, newUrl) -> {
                        // Defensive null/empty checks (fixes NPE -> worker 500)
                        if (existing == null || existing.isEmpty()) {
                            return (newUrl == null) ? "" : newUrl;
                        }
                        if (newUrl == null || newUrl.isEmpty()) {
                            return existing;
                        }
                    
                        // Build an ordered set to dedupe deterministically (associative)
                        LinkedHashSet<String> set = new LinkedHashSet<>();
                    
                        // split existing and incoming by comma, add to set
                        for (String s : existing.split(",")) {
                            if (!s.isEmpty()) set.add(s);
                        }
                        for (String s : newUrl.split(",")) {
                            if (!s.isEmpty()) set.add(s);
                        }
                    
                        // Reconstruct while enforcing MAX_POSTINGS_LENGTH
                        StringBuilder sb = new StringBuilder();
                        for (String s : set) {
                            if (sb.length() > 0) sb.append(",");
                            // if adding this entry would overflow, stop and keep current sb
                            if (sb.length() + s.length() + 1 > MAX_POSTINGS_LENGTH) {
                                logger.warn("Truncating postings while merging; reached MAX_POSTINGS_LENGTH");
                                break;
                            }
                            sb.append(s);
                        }
                        return sb.toString();
                    });
                        logger.info("foldByKey completed successfully for batch " + batchNum);
                        foldTime = System.currentTimeMillis() - foldStartTime;
                        logger.info("foldByKey completed successfully for batch " + batchNum + " (took " + foldTime + "ms)");
                        ctx.output("foldByKey() took " + foldTime + "ms\n");
                    } catch (Exception e) {
                        foldTime = System.currentTimeMillis() - foldStartTime;
                        logger.error("ERROR during foldByKey for batch " + batchNum + ": " + e.getMessage() + " (took " + foldTime + "ms)", e);
                        logger.error("Exception type: " + e.getClass().getName());
                        logger.error("Stack trace: " + java.util.Arrays.toString(e.getStackTrace()));
                        throw new RuntimeException("foldByKey failed for batch " + batchNum + ": " + e.getMessage(), e);
                    }
                    
                    if (invertedBatch == null) {
                        logger.error("invertedBatch is null after foldByKey!");
                        throw new RuntimeException("invertedBatch is null after foldByKey");
                    }
                    
                    logger.info("Successfully folded by key: combining URLs for each word");
                    //safeDestroy(wordUrlPairs, "wordUrlPairs", ctx);
                    //wordUrlPairs = null;
                    // Try writing to disc partial tables and then merge them as a second pass
                    // Short, more-unique partial table name to reduce collisions and path length
                    // Format: pip-<batch>-<last6ms>-<rand4>
                    String uniqueTableName = "pt-pip-" + batchNum + "-" + (System.currentTimeMillis() % 1_000_000) +
                                             "-" + (int)(Math.random() * 10_000);
                    logger.info("Saving batch " + batchNum + " to table: " + uniqueTableName);
                    invertedBatch.saveAsTable(uniqueTableName);
                    
                    // Log batch completion with timestamp
                    java.time.LocalDateTime now = java.time.LocalDateTime.now();
                    String timestamp = now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    logger.info("Batch " + batchNum + " completed successfully at " + timestamp);
                    ctx.output("Batch " + batchNum + " completed at " + timestamp + "\n");


                    // Collect all words in this batch
                    // TODO: See if there is a more efficient way to collect the data
                    //List<FlamePair> invertedBatchList = invertedBatch.collect();
                    //ctx.output("Processing " + invertedBatchList.size() + " unique words in batch\n");

                    // Try writing to disc partial tables and then merge them as a second pass
                    
                } finally {
                    // Ensure all RDDs are destroyed even if there's an error
                    safeDestroy(crawlRDD, "crawlRDD", ctx);
                    safeDestroy(urlPagePairs, "urlPagePairs", ctx);
                    safeDestroy(wordUrlPairs, "wordUrlPairs", ctx);
                }
                batchNum++; // Increment batch number after processing
            }
        } catch (Exception e) {
            logger.error("error with parsing pt-crawl", e);
            ctx.output("ERROR: " + e.getMessage() + "\n");
            if (e.getCause() != null) {
                ctx.output("Caused by: " + e.getCause().getMessage() + "\n");
                logger.error("Root cause: " + e.getCause().getMessage(), e.getCause());
            }
            ctx.output("Exception type: " + e.getClass().getName() + "\n");
            ctx.output("Stack trace: " + java.util.Arrays.toString(e.getStackTrace()) + "\n");
            // Print full stack trace to logger
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error("Full stack trace:\n" + sw.toString());
        }
        
        ctx.output("Indexer job completed!\n");
    }
}