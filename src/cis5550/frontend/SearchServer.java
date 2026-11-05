package cis5550.frontend;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;
import cis5550.webserver.Server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchServer {

    // --- CONFIGURATION ---
    private static final double WEIGHT_TFIDF = 5.0;
    private static final double WEIGHT_PAGERANK = 1.5;
    
    // Boosts
    private static final double BOOST_URL = 25.0;
    private static final double BOOST_TITLE = 15.0;
    private static final double BOOST_PHRASE = 30.0;
    
    private static final double PENALTY_NON_ENGLISH = 0.0001;
    private static final int RESULTS_PER_PAGE = 20;

    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en", "for", "from", 
        "how", "i", "in", "is", "it", "la", "of", "on", "or", "that", "the", "this", "to", "was", 
        "what", "when", "where", "who", "will", "with", "und", "www"
    ));

    private static final Set<String> DICTIONARY = new HashSet<>();

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[1].equals("--secure")) {
            int port = Integer.parseInt(args[0]);
            Server.securePort(port);
        } else if (args.length > 0) {
            Server.port(Integer.parseInt(args[0]));
        } else {
            Server.port(8080);
        }

        KVSClient kvs = new KVSClient("localhost:6000");
        Server.staticFiles.location("static");

        // --- LOAD DICTIONARY (Robust Path Check) ---
        System.out.println("--- Dictionary Setup ---");
        File dictFile = new File("static/dictionary.txt");
        if (!dictFile.exists()) dictFile = new File("dictionary.txt");
        
        if (dictFile.exists()) {
            System.out.println("Loading dictionary from: " + dictFile.getAbsolutePath());
            try (BufferedReader br = new BufferedReader(new FileReader(dictFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String word = line.trim().toLowerCase();
                    if (!word.isEmpty() && word.matches("^[a-z]+$")) {
                        DICTIONARY.add(word);
                    }
                }
            }
            System.out.println("SUCCESS: Loaded " + DICTIONARY.size() + " words.");
        } else {
            System.err.println("WARNING: dictionary.txt not found. Using minimal fallback.");
            DICTIONARY.addAll(Arrays.asList("kirsten", "world", "forest", "doug", "apple", "search", "engine", "web", "crawler", "java", "pennsylvania", "wharton", "computer", "science", "page", "rank", "internet", "system", "cloud", "amazon", "google", "facebook", "twitter", "instagram", "university", "creepy", "matrix", "neo", "trinity", "morpheus", "oracle", "zion", "smith"));
        }
        System.out.println("------------------------");

        Server.get("/", (req, res) -> {
            res.type("text/html");
            return "<html><head><title>Creepy Web Crawlers</title><link rel='stylesheet' href='/style.css'></head><body>" +
                    "<div style='display:flex; flex-direction:column; align-items:center; margin-top:120px;'>" +
                    "  <div style='display:flex; align-items:center; gap:15px; margin-bottom:35px;'>" +
                    "    <img src='/logo.png' height='50' alt='Spider Logo'>" +
                    "    <span class='logo-text' style='margin-right:0; font-size:42px;'>Creepy Web Crawlers</span>" +
                    "  </div>" +
                    "  <form action='/search' method='GET' style='width:100%; max-width:600px; display:flex;'>" +
                    "    <input type='text' name='query' placeholder='Search the web...' style='width:100%;'>" +
                    "    <button type='submit'>Search</button>" +
                    "  </form>" +
                    "</div><div class='footer' style='position: fixed; bottom: 0; width: 100%; text-align: center; padding: 10px; background-color: #f2f2f2; font-family: arial, sans-serif; font-size: 13px; color: #666;'>© 2025 Creepy Web Crawlers Team</div></body></html>";
        });

        Server.get("/search", (req, res) -> {
            String query = req.queryParams("query");
            if (query == null || query.isEmpty()) { res.redirect("/", 303); return null; }

            int page = 1;
            try {
                String pageParam = req.queryParams("page");
                if (pageParam != null) page = Integer.parseInt(pageParam);
            } catch (Exception e) {}
            if (page < 1) page = 1;

            String[] rawWords = query.toLowerCase().trim().split("\\s+");
            List<String> words = new ArrayList<>();
            for (String w : rawWords) {
                if (!STOP_WORDS.contains(w)) words.add(w);
            }
            if (words.isEmpty()) Collections.addAll(words, rawWords);

            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>" + query + " - Search</title>");
            html.append("<link rel='stylesheet' href='/style.css'>");
            html.append("<style>");
            html.append(".pagination { display: flex; justify-content: center; margin: 40px 0; gap: 5px; font-family: arial, sans-serif; }");
            html.append(".page-link { text-decoration: none; color: #1a0dab; padding: 8px 12px; border: 1px solid #dfe1e5; border-radius: 4px; }");
            html.append(".page-link:hover { background-color: #f1f3f4; }");
            html.append(".current-page { font-weight: bold; background-color: #e8f0fe; color: #1a0dab; padding: 8px 12px; border: 1px solid #e8f0fe; border-radius: 4px; pointer-events: none; }");
            html.append(".debug-score { font-size: 11px; color: #555; background: #f8f9fa; border: 1px solid #dadce0; padding: 4px 8px; border-radius: 4px; display: inline-block; margin-top: 4px; font-family: monospace; }");
            html.append(".did-you-mean { color: #d93025; font-size: 16px; margin-bottom: 20px; font-style: italic; }");
            html.append(".did-you-mean a { color: #1a0dab; font-weight: bold; font-style: normal; text-decoration: none; }");
            html.append(".did-you-mean a:hover { text-decoration: underline; }");
            html.append("</style>");
            html.append("</head><body>");
            
            html.append("<div class='search-header'>");
            html.append("  <a href='/' class='logo-text'><img src='/logo.png' height='32' style='margin-right:12px;'>Creepy Web Crawlers</a>");
            html.append("  <form class='search-form' action='/search' method='GET'>");
            html.append("    <input type='text' name='query' value='" + query + "'>");
            html.append("    <button type='submit'>Search</button>");
            html.append("  </form>");
            html.append("</div><div class='results-container'>");

            try {
                int totalDocs = 0;
                try { totalDocs = kvs.count("pt-crawl"); } catch(Exception e) {}
                if (totalDocs <= 0) totalDocs = 100000; 
                
                Map<String, Double> tfIdfMap = new HashMap<>();
                StringBuilder suggestedQuery = new StringBuilder();
                boolean madeCorrection = false;

                // --- PASS 1: TF-IDF & SPELL CHECK ---
                for (String w : words) {
                    boolean found = processWord(kvs, w, totalDocs, tfIdfMap, 1.0);
                    
                    if (found) {
                        suggestedQuery.append(w).append(" ");
                    } else {
                        if (DICTIONARY.contains(w)) {
                            suggestedQuery.append(w).append(" ");
                        } else {
                            String bestMatch = w;
                            int bestDist = Integer.MAX_VALUE;
                            int wLen = w.length();
                            
                            for (String dictWord : DICTIONARY) {
                                if (Math.abs(dictWord.length() - wLen) > 2) continue;
                                int dist = levenshtein(w, dictWord);
                                if (dist <= 2 && dist < bestDist) {
                                    bestDist = dist;
                                    bestMatch = dictWord;
                                }
                            }
                            if (!bestMatch.equals(w)) madeCorrection = true;
                            suggestedQuery.append(bestMatch).append(" ");
                        }
                    }
                }

                if (madeCorrection) {
                    String cleanSuggestion = suggestedQuery.toString().trim();
                    if (!cleanSuggestion.isEmpty() && !cleanSuggestion.equals(String.join(" ", words))) {
                        String encodedSuggestion = URLEncoder.encode(cleanSuggestion, StandardCharsets.UTF_8);
                        html.append("<div class='did-you-mean'>");
                        html.append("Did you mean: <a href='/search?query=" + encodedSuggestion + "'>" + cleanSuggestion + "</a>");
                        html.append("</div>");
                    }
                }

                // --- PASS 2: FETCH & RANK (WITH HASH RESOLUTION) ---
                List<SearchResult> finalResults = new ArrayList<>();
                String searchPhrase = String.join(" ", words).toLowerCase();
                
                for (String hash : tfIdfMap.keySet()) {
                    double rawTfIdf = tfIdfMap.get(hash);
                    
                    // 1. RESOLVE CRAWL DATA
                    // The hash from Index might be "h1", "h2", or "h1_h2". 
                    // pt-crawl might have any variation. We need to find it.
                    Row crawlRow = resolveCrawlRow(kvs, hash);
                    
                    String url = (crawlRow != null) ? crawlRow.get("url") : "";
                    String body = (crawlRow != null) ? crawlRow.get("page") : "";
                    if (body == null && crawlRow != null) body = crawlRow.get("value");

                    // 2. RESOLVE PAGERANK DATA
                    // PageRank MUST use the canonical "h1_h2" key generated by GraphBuilder
                    double pageRank1 = 0.0;
                    double pageRank2 = 0.0;
                    double pageRank = 0.0;
                    if (url != null && !url.isEmpty()) {
                        String canonicalKey = hashURLForRank(url); // Regenerate the exact key used in PageRank
                        Row prRow1 = kvs.getRow("pt-pageranks-1", canonicalKey);
                        Row prRow2 = kvs.getRow("pt-pageranks-2", canonicalKey);
                        if (prRow1 != null) {
                            try { pageRank1 = Double.parseDouble(prRow1.get("rank")); } catch(Exception e) {}
                        }      
                        else {
                            // Fallback: Check if the index hash happens to be the PR key (rare but possible)
                            prRow1 = kvs.getRow("pt-pageranks-1", hash);
                            if (prRow1 != null) {
                                try { pageRank = Double.parseDouble(prRow1.get("rank")); } catch(Exception e) {}
                            }
                        }
                        if (prRow2 != null) {
                            try { pageRank2 = Double.parseDouble(prRow2.get("rank")); } catch(Exception e) {}
                        }      
                        else {
                            // Fallback: Check if the index hash happens to be the PR key (rare but possible)
                            prRow2 = kvs.getRow("pt-pageranks-2", hash);
                            if (prRow2 != null) {
                                try { pageRank2 = Double.parseDouble(prRow2.get("rank")); } catch(Exception e) {}
                            }
                        }
                        // pick larger page rank
                        pageRank = Math.max(pageRank1, pageRank2);
                        
                    }

                    boolean isEnglish = true;
                    if (body != null && body.length() > 50) isEnglish = isEnglish(body);

                    String title = url; 
                    if (body != null) {
                        Pattern p = Pattern.compile("<title>(.*?)</title>", Pattern.CASE_INSENSITIVE);
                        Matcher m = p.matcher(body);
                        if (m.find()) title = m.group(1).trim().replace("&amp;", "&").replace("&#039;", "'");
                    }

                    String snippet = "No preview available.";
                    double phraseBoost = 0.0;

                    if (body != null) {
                        String cleanBody = body.replaceAll("<[^>]*>", " ").replaceAll("\\s+", " ").trim();
                        if (words.size() > 1 && cleanBody.toLowerCase().contains(searchPhrase)) {
                            phraseBoost = BOOST_PHRASE;
                        }

                        int bestIdx = -1;
                        String bestWord = "";
                        for (String w : words) {
                            int idx = cleanBody.toLowerCase().indexOf(w);
                            if (idx != -1) {
                                if (bestIdx == -1 || idx < bestIdx) {
                                    bestIdx = idx;
                                    bestWord = w;
                                }
                            }
                        }
                        
                        if (bestIdx != -1) {
                            int start = Math.max(0, bestIdx - 60);
                            int end = Math.min(cleanBody.length(), bestIdx + bestWord.length() + 60);
                            snippet = "..." + cleanBody.substring(start, end) + "...";
                            snippet = snippet.replaceAll("(?i)" + Pattern.quote(bestWord), "<b>$0</b>");
                        } else {
                            int end = Math.min(cleanBody.length(), 200);
                            snippet = cleanBody.substring(0, end) + "...";
                        }
                    }

                    double titleBoost = 0.0;
                    double urlBoost = 0.0;
                    for (String w : words) {
                        if (url.toLowerCase().contains(w)) urlBoost += BOOST_URL;
                        if (title.toLowerCase().contains(w)) titleBoost += BOOST_TITLE;
                    }

                    double totalScore = (rawTfIdf * WEIGHT_TFIDF) + (pageRank * WEIGHT_PAGERANK) + titleBoost + urlBoost + phraseBoost;
                    if (!isEnglish) totalScore *= PENALTY_NON_ENGLISH; 

                    finalResults.add(new SearchResult(hash, rawTfIdf, pageRank, totalScore, url, title, snippet, titleBoost + urlBoost + phraseBoost));
                }

                finalResults.sort((a, b) -> Double.compare(b.totalScore, a.totalScore));

                if (finalResults.isEmpty()) {
                    html.append("<p class='result-count'>No results found for <b>" + query + "</b>.</p>");
                } else {
                    int startIndex = (page - 1) * RESULTS_PER_PAGE;
                    int endIndex = Math.min(startIndex + RESULTS_PER_PAGE, finalResults.size());
                    
                    html.append("<div class='result-count'>Found " + finalResults.size() + " results (Page " + page + ")</div>");

                    for (int i = startIndex; i < endIndex; i++) {
                        SearchResult r = finalResults.get(i);
                        String decodedUrl = r.url;
                        try { decodedUrl = URLDecoder.decode(r.url, StandardCharsets.UTF_8); } catch(Exception e) {}
                        String displayUrl = decodedUrl.length() > 100 ? decodedUrl.substring(0, 97) + "..." : decodedUrl;

                        html.append("<div class='result'>");
                        html.append("  <a class='title' href='" + decodedUrl + "' target='_blank' style='font-size: 18px; color: #1a0dab; text-decoration: none; display:block;'>" + r.title + "</a>");
                        html.append("  <div class='url' style='color: #006621; font-size: 14px; margin-bottom: 2px;'>" + displayUrl + "</div>");
                        html.append("  <div class='debug-score'>");
                        html.append("    Total: <b>" + String.format("%.3f", r.totalScore) + "</b> ");
                        html.append("    | TF-IDF: " + String.format("%.3f", r.tfIdf * WEIGHT_TFIDF));
                        html.append("    | PR: " + String.format("%.3f", r.pageRank * WEIGHT_PAGERANK));
                        html.append("    | Boost: " + String.format("%.1f", r.boost));
                        html.append("  </div>");
                        html.append("  <div class='snippet' style='color: #4d5156; font-size: 14px; line-height: 1.58;'>" + r.snippet + "</div>");
                        html.append("</div>");
                        html.append("<div style='margin-bottom: 20px;'></div>"); 
                    }
                    
                    // Pagination
                    if (finalResults.size() > RESULTS_PER_PAGE) {
                        html.append("<div class='pagination'>");
                        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
                        int totalPages = (int) Math.ceil((double) finalResults.size() / RESULTS_PER_PAGE);
                        int startPage = Math.max(1, page - 5);
                        int endPage = Math.min(totalPages, page + 5);

                        if (page > 1) html.append("<a class='page-link' href='/search?query=" + encodedQuery + "&page=" + (page - 1) + "'>Previous</a>");
                        for (int p = startPage; p <= endPage; p++) {
                            if (p == page) html.append("<span class='current-page'>" + p + "</span>");
                            else html.append("<a class='page-link' href='/search?query=" + encodedQuery + "&page=" + p + "'>" + p + "</a>");
                        }
                        if (page < totalPages) html.append("<a class='page-link' href='/search?query=" + encodedQuery + "&page=" + (page + 1) + "'>Next</a>");
                        html.append("</div>");
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                html.append("<p style='color:red'>Error: " + e.getMessage() + "</p>");
            }

            html.append("</div></body></html>");
            return html.toString();
        });
    }

    // --- HELPER 1: ROBUST CRAWL LOOKUP (Splits hash if needed) ---
    // If the index has "hash1_hash2" but crawl only has "hash1", we try splitting it.
    private static Row resolveCrawlRow(KVSClient kvs, String hash) {
        try {
            // 1. Try exact match
            Row r = kvs.getRow("pt-crawl", hash);
            if (r != null) return r;

            // 2. If hash is combined (has underscore), try parts
            if (hash.contains("_")) {
                String[] parts = hash.split("_");
                // Try Part 1 (path+host)
                r = kvs.getRow("pt-crawl", parts[0]);
                if (r != null) return r;
                
                // Try Part 2 (host+path)
                if (parts.length > 1) {
                    r = kvs.getRow("pt-crawl", parts[1]);
                    if (r != null) return r;
                }
            }
        } catch (Exception e) {}
        return null;
    }

    // --- HELPER 2: GENERATE CANONICAL RANK KEY ---
    // This MUST match GraphBuilder's logic to find the PageRank
    private static String hashURLForRank(String url) {
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
            
            // Double-Normalization Logic used in GraphBuilder
            String seed1 = canonicalPath + canonicalHost;
            String seed2 = canonicalHost + canonicalPath;
            
            return Hasher.hash(seed1) + "_" + Hasher.hash(seed2); // NOTE: Uses UNDERSCORE
        } catch (Exception e) {
            return Hasher.hash(url);
        }
    }

    private static boolean processWord(KVSClient kvs, String w, int totalDocs, Map<String, Double> tfIdfMap, double multiplier) {
        try {
            Row indexRow1 = kvs.getRow("pt-index-1", w);
            Row indexRow2 = kvs.getRow("pt-index-2", w);

            // go through pt-index-1 and pt-index-2 and combine the results into a row called indexRow
            String csvData1 = "";
            if (indexRow1 != null) {
                for (String col : indexRow1.columns()) {
                    String val = indexRow1.get(col);
                    if (val != null && !val.isEmpty()) {
                        csvData1 = val;
                            break;
                        }
                    }
                }
            
            String csvData2 = "";
            if (indexRow2 != null) {
                for (String col : indexRow2.columns()) {
                    String val = indexRow2.get(col);
                    if (val != null && !val.isEmpty()) {
                        csvData2 = val;
                            break;
                    }
                }
            }
            // if word does not exist in either index, return false
            if ((csvData1 == null || csvData1.isEmpty()) &&
                (csvData2 == null || csvData2.isEmpty())) {
                return false;
            }
            // sort out unique values from csvData1 and csvData2
            Set<String> wordTF = new HashSet<>();
            wordTF.addAll(Arrays.asList(csvData1.split(",")));
            wordTF.addAll(Arrays.asList(csvData2.split(",")));
            double idf = Math.log((double) totalDocs / wordTF.size());

            // calculate tf-idf for each url hash
            for (String entry : wordTF) {
                String[] parts = entry.split(":");
                if (parts.length < 2) continue;
                String urlHash = parts[0];
                double tf = Double.parseDouble(parts[1]);
                tfIdfMap.put(urlHash, tfIdfMap.getOrDefault(urlHash, 0.0) + (tf * idf * multiplier));
            }
            return true;
        } catch (Exception e) {}
        return false;
    }

    private static boolean isEnglish(String text) {
        int asciiCount = 0;
        int totalChars = text.length();
        for (int i = 0; i < totalChars; i++) {
            if (text.charAt(i) < 128) asciiCount++;
        }
        double asciiRatio = (double) asciiCount / totalChars;
        if (asciiRatio < 0.70) return false; 
        String[] words = text.toLowerCase().split("\\s+");
        int stopWordCount = 0;
        int maxWordsToCheck = Math.min(words.length, 500); 
        for (int i = 0; i < maxWordsToCheck; i++) {
            if (STOP_WORDS.contains(words[i])) stopWordCount++;
        }
        double stopWordRatio = (double) stopWordCount / maxWordsToCheck;
        return stopWordRatio > 0.05; 
    }

    private static int levenshtein(String a, String b) {
        int[] costs = new int[b.length() + 1];
        for (int j = 0; j < costs.length; j++) costs[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            costs[0] = i;
            int nw = i - 1;
            int j_lim = b.length();
            for (int j = 1; j <= j_lim; j++) {
                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
                nw = costs[j];
                costs[j] = cj;
            }
        }
        return costs[b.length()];
    }

    static class SearchResult {
        String hash;
        double tfIdf;
        double pageRank;
        double totalScore;
        String url;
        String title;
        String snippet;
        double boost;

        public SearchResult(String h, double tf, double pr, double tot, String u, String t, String s, double b) {
            this.hash = h; this.tfIdf = tf; this.pageRank = pr; this.totalScore = tot;
            this.url = u; this.title = t; this.snippet = s; this.boost = b;
        }
    }
}