package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class Crawler {
    private static final Logger logger = Logger.getLogger(Crawler.class);
    
    // Frontier persistence - each URL is a separate row
    private static final String FRONTIER_TABLE = "pt-crawler-frontier";
    
    // Spider trap detection
    private static final int MAX_URL_LENGTH = 500; // Prevent very long URLs (often traps)
    private static final int MAX_PAGES_PER_DOMAIN = 3000; // Limit pages per domain
    private static final int MAX_URL_DEPTH = 50; // Maximum path segments in URL
    
    // Queue prioritization - can change these #s, just chose 100, 3, and 80 as starting points
    private static final int MAX_PATH_LENGTH = 100;
    private static final int MAX_QUERY_PARAMS = 3;
    private static final int MAX_QUERY_LENGTH = 80;
    
    // Network timeouts and limits
    private static final int CONNECT_TIMEOUT_MS = 1000;  // 1 second to connect
    private static final int READ_TIMEOUT_MS = 3000;     // 3 seconds to read response
    private static final int MAX_PAGE_SIZE = 5_000_000;  // 5MB max page size

    public static void run(FlameContext ctx, String[] args) {
        if (args.length != 1) {
            ctx.output("error: crawler input does not contain correct arg (seed url)");
        } else {
            ctx.output("OK");
        }

        FlameRDD urlQueue = null;
        KVSClient mainClient = null;
        
        try {
            mainClient = ctx.getKVS();
            
            // // Try to restore frontier from previous run
            // List<String> restoredUrls = restoreFrontier(mainClient);
            
            // if (restoredUrls != null && !restoredUrls.isEmpty()) {
            //     System.out.println("Restoring frontier with " + restoredUrls.size() + " URLs");
            //     try {
            //         urlQueue = ctx.parallelize(restoredUrls);
            //     } catch (Exception e) {
            //         System.out.println("Error parallelizing restored urls, starting fresh: " + e.getMessage());
            //         // Fall back to seed URL if restoration fails
            //         String seedURL = normalizeURL(args[0], "");
            //         urlQueue = ctx.parallelize(List.of(seedURL));
            //         addToFrontier(mainClient, seedURL);
            //     }
            // } else {
                System.out.println("Starting fresh crawl from seed URL");
                String seedURL = normalizeURL(args[0], "");
                
                // Create multiple copies of seed URL to distribute across all workers
                // This ensures all workers get work from the start
                List<String> initialUrls = new ArrayList<>();
                for (int i = 0; i < 20; i++) {  // 20 copies to ensure good distribution
                    initialUrls.add(seedURL);
                }
                urlQueue = ctx.parallelize(initialUrls);
                // Add seed URL to frontier
                // addToFrontier(mainClient, seedURL);
            // }

            int iterationCount = 0;
            int maxIterations = 10000;  // Very high limit - let it crawl as long as needed
            
            System.out.println("Beginning crawl loop");
            while (iterationCount < maxIterations) {
                int currentQueueSize = urlQueue.count();
                System.out.println("[DEBUG] Iteration " + iterationCount + ": Queue size = " + currentQueueSize);
                
                if (currentQueueSize == 0) {
                    System.out.println("Queue is empty, stopping crawl");
                    break;
                }

                iterationCount++;
                String kvsCoordinator = ctx.getKVS().getCoordinator();
                FlameRDD nextQueue = urlQueue.flatMap((url) -> {
                    List<String> newURLs = new ArrayList<>();
                    // Reuse KVS client connection - create once per worker task, not per URL
                    // This is captured from the enclosing scope and reused across URLs
                    KVSClient client = null;
                    // Catch all errors for each URL so the worker doesn't throw 500 and kill the crawl.
                    try {
                    client = new KVSClient(kvsCoordinator);
                    
                    // Fast URL-based spider trap checks (no KVS operations)
                    if (url.length() > MAX_URL_LENGTH) {
                        return Collections.emptyList();
                    }
                    
                    // Parse URL once
                    String[] parts = URLParser.parseURL(url);
                    String host = parts[1];
                    String path = parts[3];
                    
                    // Check URL depth
                    if (path != null) {
                        int segmentCount = path.split("/").length;
                        if (segmentCount > MAX_URL_DEPTH) {
                            return Collections.emptyList();
                        }
                    }
                    
                    // Check for calendar/trap patterns
                    String lowerUrl = url.toLowerCase();
                    if (lowerUrl.matches(".*(year=|month=|day=|date=|calendar).*") &&
                        lowerUrl.matches(".*\\d{4}.*")) {
                        return Collections.emptyList();
                    }
                    
                    // Dedup check - skip if already crawled (prevents wasted HTTP requests)
                    String repeatCheck = hashURLForDistribution(url);
                    if (client.existsRow("pt-crawl", repeatCheck)) {
                        return Collections.emptyList();
                    }
                    
                    // Get host row ONCE - check robots, page count, etc.
                    Row hostRow = client.getRow("hosts", host);
                    
                    // Check per-domain page limit (spider trap check)
                    if (hostRow != null && hostRow.get("pageCount") != null) {
                        int pageCount = Integer.parseInt(hostRow.get("pageCount"));
                        if (pageCount >= MAX_PAGES_PER_DOMAIN) {
                            return Collections.emptyList();
                        }
                    }

                    long now = System.currentTimeMillis();

                    // Declare HEAD connection outside try block so it's accessible in catch blocks
                    HttpURLConnection head = null;
                    int rCode = -1;
                    String cType = null;
                    String length = null;

                    try {
                        // step 9 check robots


                        String robotContent = null;
                        if (hostRow == null || hostRow.get("robotsFetched") == null) {
                            // we havent fetched robots yet, so fetch it once now
                            String robotsUrl = parts[0] + "://" + host + (parts[2].isEmpty() ? "" : ":" + parts[2]) + "/robots.txt";
                            HttpURLConnection robotsConn = null;
                            try {
                                robotsConn = (HttpURLConnection) new URL(robotsUrl).openConnection();
                                robotsConn.setConnectTimeout(1000);  // 1s for robots.txt
                                robotsConn.setReadTimeout(2000);     // 2s for robots.txt
                                robotsConn.setRequestMethod("GET");
                                robotsConn.setRequestProperty("User-Agent", "cis5550-crawler");
                                robotsConn.connect();

                                if (robotsConn.getResponseCode() == 200) {
                                    BufferedReader br = new BufferedReader(new InputStreamReader(robotsConn.getInputStream()));
                                    StringBuilder sb = new StringBuilder();
                                    String line;
                                    int lineCount = 0;
                                    // Limit robots.txt reading to 500 lines max
                                    while ((line = br.readLine()) != null && lineCount++ < 500)
                                        sb.append(line).append("\n");
                                    br.close();
                                    robotContent = sb.toString();
                                } else {
                                    robotContent = "";
                                }
                                // On success: don't disconnect - allows connection reuse (keep-alive)
                            } catch (Exception e) {
                                logger.error("Error fetching robots.txt for " + host + ": " + e.getMessage(), e);
                                robotContent = "";  // Default to allowing all if robots.txt can't be fetched
                                if (robotsConn != null) robotsConn.disconnect();  // Disconnect on error
                            }

                            Row newHost = new Row(host);
                            if (hostRow != null && hostRow.get("lastAccess") != null)
                                newHost.put("lastAccess", hostRow.get("lastAccess"));
                            // Initialize page count
                            int pageCount = (hostRow != null && hostRow.get("pageCount") != null) ? 
                                Integer.parseInt(hostRow.get("pageCount")) : 0;
                            newHost.put("pageCount", Integer.toString(pageCount));
                            newHost.put("robots", robotContent);
                            newHost.put("robotsFetched", "true");
                            newHost.put("lastAccess", Long.toString(now));
                            // batch put
                            for (String col : newHost.columns()) {
                                client.putBatched("hosts", newHost.key(), col, newHost.getBytes(col));
                            }
                            // Update our cached hostRow for page count check below
                            hostRow = newHost;
                        } else {
                            robotContent = hostRow.get("robots");
                        }
                        // check if this path is allowed

                        if (!robotAllowed(robotContent, path)) {
                            return Collections.emptyList();
                        }

                        // Skip lastAccess update - not critical, wastes time
                        // Hosts table is only updated when fetching robots

                    } catch (java.net.UnknownHostException e) {
                        logger.error("DNS error for url " + url + " (robots.txt)", e);
                        return Collections.emptyList();
                    } catch (javax.net.ssl.SSLHandshakeException e) {
                        logger.error("Certificate error for url " + url + " (robots.txt)", e);
                        return Collections.emptyList();
                    } catch (java.net.SocketTimeoutException | java.net.ConnectException e) {
                        logger.error("Server connection error for url " + url + " (robots.txt)", e);
                        return Collections.emptyList();
                    } catch (Exception e) {
                        logger.error("Error during robots.txt processing: " + url, e);
                        return Collections.emptyList();
                    }

                    // // GET request (single request - no HEAD!)
                    // HttpURLConnection conn = null;
                    // try {
                    //     conn = (HttpURLConnection) new URL(url).openConnection();
                    //     conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
                    //     conn.setReadTimeout(READ_TIMEOUT_MS);
                    //     conn.setInstanceFollowRedirects(false);
                    //     conn.setRequestMethod("GET");
                    //     conn.setRequestProperty("User-Agent", "cis5550-crawler");
                    //     conn.connect();
                        
                    //     rCode = conn.getResponseCode();
                    //     cType = conn.getContentType();
                    //     length = conn.getHeaderField("Content-Length");
                        
                    //     // Handle redirects
                    //     if (rCode == 301 || rCode == 302 || rCode == 303 || rCode == 307 || rCode == 308) {
                    //         Row redirectRow = new Row(hashURLForDistribution(url));
                    //         redirectRow.put("url", url);
                    //         redirectRow.put("responseCode", Integer.toString(rCode));
                    //         if (cType != null) redirectRow.put("contentType", cType);
                    //         if (length != null) redirectRow.put("length", length);
                    //         for (String col : redirectRow.columns()) {
                    //             client.putBatched("pt-crawl", redirectRow.key(), col, redirectRow.getBytes(col));
                    //         }
                    //         String loc = conn.getHeaderField("Location");
                    //         if (loc != null) {
                    //             String newUrl = normalizeURL(url, loc);
                    //             if (newUrl != null) {
                    //                 return List.of(newUrl);
                    //             }
                    //         }
                    //         return Collections.emptyList();
                    //     }
                        
                    //     // Handle errors
                    //     if (rCode >= 500 && rCode < 600 || rCode >= 400 && rCode < 500 || rCode != 200) {
                    //         return Collections.emptyList();
                    //     }
                        
                    //     // Check content type BEFORE reading body
                    //     if (cType == null || !cType.toLowerCase().startsWith("text/html")) {
                    //         return Collections.emptyList();
                    //     }
                        
                    //     // Check content length if available
                    //     if (length != null) {
                    //         try {
                    //             int contentLength = Integer.parseInt(length);
                    //             if (contentLength > MAX_PAGE_SIZE) {
                    //                 return Collections.emptyList();
                    //             }
                    //         } catch (NumberFormatException e) {
                    //             // Ignore parse errors
                    //         }
                    //     }
                        
                    //     // NOW read the page body with size limit
                    //     InputStream in = conn.getInputStream();

                    // head request first to check if we should fetch
                    try {
                        head = (HttpURLConnection) new URL(url).openConnection();
                        head.setConnectTimeout(CONNECT_TIMEOUT_MS);
                        head.setReadTimeout(READ_TIMEOUT_MS);
                        head.setInstanceFollowRedirects(false);
                        head.setRequestMethod("HEAD");
                        head.setRequestProperty("User-Agent", "cis5550-crawler");
                        head.connect();

                        rCode = head.getResponseCode();
                        cType = head.getContentType();
                        length = head.getHeaderField("Content-Length");
                    } catch (java.net.SocketTimeoutException | java.net.ConnectException e) {
                        logger.error("Server connection error for url " + url, e);
                        if (head != null) head.disconnect();
                        return Collections.emptyList();
                    } catch (Exception e) {
                        logger.error("Error with HEAD request from crawler", e);
                        if (head != null) head.disconnect();
                        return Collections.emptyList();
                    }

                    // record head response info
                    Row r = new Row(hashURLForDistribution(url));
                    r.put("url", url);
                    r.put("responseCode", Integer.toString(rCode));
                    if (cType != null) {
                        r.put("contentType", cType);
                    }
                    if (length != null) {
                        r.put("length", length);
                    }
                    try {
                        for (String col : r.columns()) {
                            client.putBatched("pt-crawl", r.key(), col, r.getBytes(col));
                        }
                    } catch (Exception e) {
                        logger.error("Failed to put row in 'pt-crawl': " + e.getMessage(), e);
                    }

                    // handle redirects
                    if (rCode == 301 || rCode == 302 || rCode == 303 || rCode == 307 || rCode == 308) {
                        String loc = head.getHeaderField("Location");
                        if (loc != null) {
                            String newUrl = normalizeURL(url, loc);
                            if (newUrl != null) {
                                return List.of(newUrl);
                            }
                        }
                        return Collections.emptyList();
                    }

                    // handle errors, skip downloading body
                    if (rCode >= 500 && rCode < 600 || rCode >= 400 && rCode < 500 || rCode != 200) {
                        return Collections.emptyList();
                    }

                    // check content type before downloading body
                    if (cType == null || !cType.toLowerCase().startsWith("text/html")) {
                        return Collections.emptyList();
                    }

                    // check content length if available
                    if (length != null) {
                        try {
                            int contentLength = Integer.parseInt(length);
                            if (contentLength > MAX_PAGE_SIZE) {
                                return Collections.emptyList();
                            }
                        } catch (NumberFormatException e) {
                            // ignore parse errors
                        }
                    }

                    // get request, only fetch body if head indicates it's html
                    HttpURLConnection conn = null;
                    try {
                        conn = (HttpURLConnection) new URL(url).openConnection();
                        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
                        conn.setReadTimeout(READ_TIMEOUT_MS);
                        conn.setInstanceFollowRedirects(false);
                        conn.setRequestMethod("GET");
                        conn.setRequestProperty("User-Agent", "cis5550-crawler");
                        conn.connect();
                        
                        // verify we still get 200 (should match HEAD)
                        if (conn.getResponseCode() != 200) {
                            if (conn != null) conn.disconnect();
                            return Collections.emptyList();
                        }
                        
                        // read the page body with size limit
                        InputStream in = conn.getInputStream();

                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        byte[] data = new byte[8192];
                        int nRead;
                        int totalRead = 0;
                        while ((nRead = in.read(data, 0, data.length)) != -1) {
                            totalRead += nRead;
                            if (totalRead > MAX_PAGE_SIZE) {
                                in.close();
                                return Collections.emptyList();  // Skip oversized pages
                            }
                            buffer.write(data, 0, nRead);
                        }
                        in.close();
                        String page = buffer.toString();

                            // content level dedup (w/ shingling)
                            // String contentHash = contentHash(page);

                            // // check if we've already seen this content
                            /* 
                            // Row dup = client.getRow("pt-content", contentHash);
                            // if (dup != null) {
                            //     // don't store again, don't follow its links
                            //     return Collections.emptyList();
                            // }*/ 

                            // // if we haven't seen it yet, record this page's contentHash
                            // Row chRow = new Row(contentHash);
                            // chRow.put("url", url);
                            // // batch put
                            /*
                            // for (String col : chRow.columns()) {
                            //     client.putBatched("pt-content", chRow.key(), col, chRow.getBytes(col));
                            // }*/

                            Row headRow = new Row(hashURLForDistribution(url));
                            headRow.put("url", url);
                            headRow.put("page", page);
                            headRow.put("responseCode", Integer.toString(conn.getResponseCode()));
                            // i think the autograder wanted this idk
                            String contentType = conn.getContentType();
                            if (contentType != null)
                                headRow.put("contentType", contentType);
                            headRow.put("length", Integer.toString(page.length()));
                            try {
                                    // batch put
                                    for (String col : headRow.columns()) {
                                        client.putBatched("pt-crawl", headRow.key(), col, headRow.getBytes(col));
                                    }
                                    
                                    // Increment page count inline (avoid extra getRow)
                                    if (hostRow != null) {
                                        int currentCount = hostRow.get("pageCount") != null ? 
                                            Integer.parseInt(hostRow.get("pageCount")) : 0;
                                        hostRow.put("pageCount", Integer.toString(currentCount + 1));
                                        for (String col : hostRow.columns()) {
                                            client.putBatched("hosts", hostRow.key(), col, hostRow.getBytes(col));
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error("putRow failed: " + e.getMessage(), e);
                                    throw e; // Re-throw to see the actual error
                                }



                        List<String> extracted = extractURLs(page.toString());
                        List<String> normalized = new ArrayList<>();
                        for (String link : extracted) {
                            String norm = normalizeURL(url, link);
                            if (norm == null) continue;
                            if (!((norm.startsWith("http://") || norm.startsWith("https://"))))
                                continue;
                            // Fast string checks instead of regex
                            String lower = norm.toLowerCase();
                            if (lower.endsWith(".jpg") || lower.endsWith(".jpeg") || 
                                lower.endsWith(".png") || lower.endsWith(".gif") || 
                                lower.endsWith(".txt") || lower.contains(".jpg?") || 
                                lower.contains(".jpeg?") || lower.contains(".png?") || 
                                lower.contains(".gif?") || lower.contains(".txt?"))
                                continue;
                            // added for queue prioritization
                            if (!shouldEnqueue(norm)) 
                                continue;
                            if (norm != null) {
                                normalized.add(norm);
                                // Add to frontier immediately when discovered
                                // addToFrontier(client, norm);
                            }
                        }
                        newURLs.addAll(normalized);
                        // On success: don't disconnect - allows connection reuse (keep-alive)
                    } catch (java.net.UnknownHostException e) {
                        logger.error("DNS error for url " + url + " (GET)", e);
                        // recordError(client, url, "dns", e.getMessage());
                        if (conn != null) conn.disconnect();  // Disconnect on DNS error
                        return Collections.emptyList();
                    } catch (javax.net.ssl.SSLHandshakeException e) {
                        logger.error("Certificate error for url " + url + " (GET)", e);
                        // recordError(client, url, "cert", e.getMessage());
                        if (conn != null) conn.disconnect();  // Disconnect on SSL error
                        return Collections.emptyList();
                    } catch (java.net.SocketTimeoutException | java.net.ConnectException e) {
                        logger.error("Server connection error for url " + url + " (GET)", e);
                        // recordError(client, url, "server", e.getClass().getSimpleName());
                        if (conn != null) conn.disconnect();  // Disconnect on timeout/error
                        return Collections.emptyList();
                    } catch (Exception e) {
                        logger.error("Error with GET request from crawler for url " + url, e);
                        // recordError(client, url, "other", e.getClass().getSimpleName());
                        if (conn != null) conn.disconnect();  // Disconnect on error
                        return Collections.emptyList();
                    }
                    return newURLs;
                } catch (Exception e) {
                    logger.error("Error processing URL in flatMap: " + url, e);
                    return Collections.emptyList();
                } finally {
                    // Flush this URL's batched operations
                    if (client != null) {
                        try {
                            client.flush();
                        } catch (Exception e) {
                            logger.error("Failed to flush KVS client for URL: " + url, e);
                        }
                    }
                }
                });

                urlQueue = nextQueue;
                
                // Flush batches at iteration boundaries (not per URL!)
                // This ensures data is written while maintaining batching benefits
                try {
                    if (mainClient != null) {
                        mainClient.flush();
                    }
                } catch (Exception e) {
                    logger.error("Failed to flush at iteration boundary", e);
                }
                
                // No sleep - let Flame workers process as fast as possible
            }
            
            // Crawl completed - frontier should be empty now
            System.out.println("Crawl completed successfully!");
        } catch (Exception e) {
            logger.error("exception with crawler run method", e);
            // Frontier is already up-to-date in KVS from real-time updates
            System.err.println("Crawler stopped with error. Frontier preserved in KVS.");
        }

        // debug
        System.out.println("QUEUE IS EMPTY!");
    }

    private static void addToFrontier(KVSClient client, String url) {
        try {
            String urlHash = hashURLForDistribution(url);
            Row frontierRow = new Row(urlHash);
            frontierRow.put("url", url);
            frontierRow.put("added", Long.toString(System.currentTimeMillis()));
            // batch put
            for (String col : frontierRow.columns()) {
                client.putBatched(FRONTIER_TABLE, frontierRow.key(), col, frontierRow.getBytes(col));
            }
        } catch (Exception e) {
            logger.error("Failed to add URL to frontier: " + url, e);
        }
    }

    private static void removeFromFrontier(KVSClient client, String url) {
        try {
            String urlHash = hashURLForDistribution(url);
            // Remove by putting an empty row (KVS doesn't have a delete row method)
            Row emptyRow = new Row(urlHash);
            // batch put
            for (String col : emptyRow.columns()) {
                client.putBatched(FRONTIER_TABLE, emptyRow.key(), col, emptyRow.getBytes(col));
            }
        } catch (Exception e) {
            logger.error("Failed to remove URL from frontier: " + url, e);
        }
    }

    private static List<String> restoreFrontier(KVSClient client) {
        try {
            List<String> urls = new ArrayList<>();
            // Iterate through all rows in the frontier table
            Iterator<Row> iter = client.scan(FRONTIER_TABLE);
            while (iter.hasNext()) {
                Row row = iter.next();
                String url = row.get("url");
                if (url != null && !url.isEmpty()) {
                    urls.add(url);
                }
            }
            if (!urls.isEmpty()) {
                logger.info("Restored frontier with " + urls.size() + " URLs");
            }
            return urls.isEmpty() ? null : urls;
        } catch (Exception e) {
            logger.error("Failed to restore frontier", e);
            return null;
        }
    }

    private static boolean isSpiderTrap(String url, KVSClient client) {
        try {
            // Check 1: URL length limit
            if (url.length() > MAX_URL_LENGTH) {
                logger.info("Skipping URL (too long): " + url.substring(0, 100) + "...");
                return true;
            }

            // Check 2: URL depth (too many path segments)
            String[] parts = URLParser.parseURL(url);
            String path = parts[3];
            if (path != null) {
                int segmentCount = path.split("/").length;
                if (segmentCount > MAX_URL_DEPTH) {
                    logger.info("Skipping URL (too deep): " + url);
                    return true;
                }
            }

            // Check 3: Per-domain page limit
            String host = parts[1];
            if (host != null) {
                // Count pages already crawled from this domain
                Row hostRow = client.getRow("hosts", host);
                if (hostRow != null && hostRow.get("pageCount") != null) {
                    int pageCount = Integer.parseInt(hostRow.get("pageCount"));
                    if (pageCount >= MAX_PAGES_PER_DOMAIN) {
                        logger.info("Skipping URL (domain limit reached): " + url);
                        return true;
                    }
                }
            }

            // Check 4: Detect common trap patterns (calendar dates, session IDs, etc.)
            String lowerUrl = url.toLowerCase();
            if (lowerUrl.matches(".*(year=|month=|day=|date=|calendar).*") &&
                lowerUrl.matches(".*\\d{4}.*")) { // Has a 4-digit year
                logger.info("Skipping URL (calendar pattern): " + url);
                return true;
            }

            // Check 5: Repeating path segments (e.g., /a/a/a/a/a)
            if (path != null && path.length() > 20) {
                String[] segments = path.split("/");
                if (segments.length > 5) {
                    // Check if same segment repeats more than 3 times
                    Map<String, Integer> segmentCount = new HashMap<>();
                    for (String seg : segments) {
                        if (!seg.isEmpty()) {
                            segmentCount.put(seg, segmentCount.getOrDefault(seg, 0) + 1);
                            if (segmentCount.get(seg) > 3) {
                                logger.info("Skipping URL (repeating segments): " + url);
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        } catch (Exception e) {
            logger.error("Error checking spider trap for URL: " + url, e);
            return false; // Don't skip on error
        }
    }

    private static void incrementDomainPageCount(KVSClient client, String host) {
        try {
            Row hostRow = client.getRow("hosts", host);
            if (hostRow == null) {
                hostRow = new Row(host);
            }
            int currentCount = hostRow.get("pageCount") != null ? 
                Integer.parseInt(hostRow.get("pageCount")) : 0;
            hostRow.put("pageCount", Integer.toString(currentCount + 1));
            // batch put
            for (String col : hostRow.columns()) {
                client.putBatched("pt-error", hostRow.key(), col, hostRow.getBytes(col));
            }
        } catch (Exception e) {
            logger.error("Error incrementing domain page count", e);
        }
    }

    private static boolean robotAllowed(String robots, String path) {
        if (robots == null || robots.isEmpty()) {
            return true;
        }
        String[] lines = robots.split("\n");

        boolean found = false;

        for (String line : lines) {
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            if (line.toLowerCase().startsWith("user-agent: ")) {
                String agentName = line.substring(11).trim().toLowerCase();
                if (agentName.equals("cis5550-crawler") || agentName.equals("*")) {
                    found = true;
                }
            } else if (found) {
                if (line.toLowerCase().startsWith("disallow:")) {
                    String instruction = line.substring(9).trim();
                    if (!instruction.isEmpty() && path.startsWith(instruction)) {
                        return false;
                    }
                } else if (line.toLowerCase().startsWith("allow:")) {
                    String instruction = line.substring(6).trim();
                    if (path.startsWith(instruction)) {
                        return true;
                    }
                }
            }
        }
        return true;
    }
    public static List<String> extractURLs(String html) {
        List<String> urls = new ArrayList<>();
        String lower = html.toLowerCase();
        int i = 0;

        while (true) {
            int startTag = lower.indexOf('<', i);
            if (startTag == -1)
                break;
            int endTag = lower.indexOf('>', startTag);
            if (endTag == -1)
                break;

            if (startTag + 1 >= html.length() || endTag > html.length()) {
            i = endTag + 1;
            continue;
        }


            String tag = html.substring(startTag + 1, endTag).trim();
            i = endTag + 1;

            if (tag.startsWith("/"))
                continue;

            String[] parts = tag.split("\\s+");
            if (parts.length == 0)
                continue;

            String tagName = parts[0].toLowerCase();
            if (!tagName.equals("a"))
                continue;

            // getting href
            for (String part : parts) {
                if (part.toLowerCase().startsWith("href=")) {
                    String val = part.substring(5).trim();
                    if (val.length() > 0) {
                    if ((val.startsWith("\"") && val.endsWith("\"")) || 
                        (val.startsWith("'") && val.endsWith("'"))) {
                        if (val.length() > 2) {
                            val = val.substring(1, val.length() - 1);
                        } else {
                            continue;
                        }
                    }
                    urls.add(val);
                    }
                }
            }
        }

        return urls;
    }

    public static String normalizeURL(String base, String href) {
        try {
            if (href == null)  {
                href = "";
            }

            href = href.trim();

            int hash = href.indexOf('#');
            if (hash != -1) {
                href = href.substring(0, hash);
            }
//            if (href.isEmpty()) {
//                return null;
//            }

            // to prevent unknown protocol exception
            String lower = href.toLowerCase();
            if (lower.startsWith("javascript") || lower.startsWith("tel") || lower.startsWith("ttps") || 
            lower.equals("irc") || lower.startsWith("wikimedia")) {
                return null;
            }

            // resolve relative url issues
            URL absoluteURL = href.isEmpty() ? new URL(base) : new URL(new URL(base), href);
            String full = absoluteURL.toString();

            // get url parts
            String[] parts = URLParser.parseURL(full);
            String protocol = parts[0];
            String host = parts[1];
            String port = parts[2];
            String path = parts[3];

            if (parts == null || parts.length < 4) {
                return null;
            }

            // to prevent null pointer exceptions
            if (protocol == null || host == null) {
                return null;
            }

            if (port == null || port.isEmpty()) {
                port = protocol.equals("https") ? "443" : "80";
            }
            if (path == null || path.isEmpty())
                path = "/";
            return protocol + "://" + host + ":" + port + path;
        } catch (Exception e) {
            logger.error("Error normalizing url", e);
            return null;
        }
    }

    // create a table to add urls that resulted in errors, so we don't crawl them again 
    private static void recordError(KVSClient client, String url, String errorType, String detail) {
        try {
            Row r = new Row(hashURLForDistribution(url));
            r.put("url", url);
            // e.g. "dns", "cert", "server", "broken-link"
            r.put("errorType", errorType);
            if (detail != null) {
                // exception message or HTTP code
                r.put("detail", detail);
            }
            // batch puts
            for (String col : r.columns()) {
                client.putBatched("pt-error", r.key(), col, r.getBytes(col));
            }
            // Remove from frontier after recording error
            removeFromFrontier(client, url);
        } catch (Exception e) {
            logger.error("Failed to record error for url " + url, e);
        }
    }

    private static boolean shouldEnqueue(String url) {
        if (pathTooLong(url)) {
            return false;
        }
        if (tooManyQueryParams(url)) {
            return false;
        }
        // Filter non-English Wikipedia pages
        if (url.contains("wikipedia.org")) {
            // Only allow en.wikipedia.org, block all other language codes
            if (!url.contains("://en.wikipedia.org/")) {
                return false;
            }
        }
        return true;
    }

    private static boolean pathTooLong(String urlStr) {
        try {
            URL u = new URL(urlStr);
            String path = u.getPath();
            if (path == null) {
                return false;
            }
            return path.length() > MAX_PATH_LENGTH;
        } catch (MalformedURLException e) {
            // url was weird we already filtered badly formed stuff earlier so don't block just because of this
            return false;
        }
    }

    private static boolean tooManyQueryParams(String urlStr) {
        try {
            URL u = new URL(urlStr);
            String query = u.getQuery(); 
            if (query == null || query.isEmpty()) {
                return false;
            }

            // cap total query length (prevents super long junk)
            if (query.length() > MAX_QUERY_LENGTH) {
                return true;
            }

            // cap number of individual params
            String[] parts = query.split("&");
            if (parts.length > MAX_QUERY_PARAMS) {
                return true;
            }

            // block obviously junky param names
            String lower = query.toLowerCase();
            if (lower.contains("sessionid") || lower.contains("jsessionid") || lower.contains("utm_")) {
                return true;
            }

            return false;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    // shingling to dedup at the content level - optimized to only use title + snippet
    private static String contentHash(String page) {
        // extract title
        String title = "";
        int titleStart = page.toLowerCase().indexOf("<title>");
        int titleEnd = page.toLowerCase().indexOf("</title>");
        if (titleStart != -1 && titleEnd != -1 && titleEnd > titleStart) {
            title = page.substring(titleStart + 7, titleEnd).trim();
        }
        
        // extract first 500 ish chars of body text (after stripping tags)
        String bodySnippet = page.replaceAll("(?s)<[^>]*>", " ");
        bodySnippet = bodySnippet.trim();
        if (bodySnippet.length() > 500) {
            bodySnippet = bodySnippet.substring(0, 500);
        }
        
        // combine title + snippet
        String combined = (title + " " + bodySnippet).trim();
        
        if (combined.isEmpty()) {
            return Hasher.hash("");
        }
        
        // normalize
        String text = combined.toLowerCase().replaceAll("[^a-z0-9]+", " ").trim();
        
        // split into words
        String[] words = text.split("\\s+");
        if (words.length == 0) {
            return Hasher.hash(text);
        }
        
        // 5 word shingles
        int k = 5;
        List<Integer> shingleHashes = new ArrayList<>();
        
        // Build overlapping k-word shingles
        for (int i = 0; i + k <= words.length; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < k; j++) {
                if (j > 0) {
                    sb.append(' ');
                }
                sb.append(words[i + j]);
            }
            shingleHashes.add(sb.toString().hashCode());
        }
        
        if (shingleHashes.isEmpty()) {
            return Hasher.hash(text);
        }
        
        // take smallest 50 as signature
        Collections.sort(shingleHashes);
        int limit = Math.min(50, shingleHashes.size());
        
        StringBuilder signature = new StringBuilder();
        for (int i = 0; i < limit; i++) {
            signature.append(shingleHashes.get(i)).append(',');
        }
        
        return Hasher.hash(signature.toString());
    }

    private static String hashURLForDistribution(String url) {
        try {
            String[] parts = URLParser.parseURL(url);
            if (parts == null || parts.length < 4) return Hasher.hash(url);   

            String host = parts[1];
            String path = parts[3];

            if (host == null) return Hasher.hash(url);

            // Hash path+host instead of host+path for better distribution
            // This spreads pages from same domain across different KVS workers
            String toHash = (path != null ? path : "/") + host;
            return Hasher.hash(toHash);
        } catch (Exception e) {
            return Hasher.hash(url);
        }
    }
}