package cis5550.jobs;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.URLParser;


public class Crawler_cam {
	
	public static List<String> extractedUrls(String page) {
	    List<String> urls = new ArrayList<>();

	    if (page == null || page.isEmpty()) {
	        return urls;
	    }

	    // Convert to lowercase for easier matching (but keep original for URLs)
	    String lower = page.toLowerCase(Locale.ROOT);
	    int index = 0;

	    while (true) {
	        // Find next tag
	        int startTag = lower.indexOf('<', index);
	        if (startTag == -1) break;
	        int endTag = lower.indexOf('>', startTag + 1);
	        if (endTag == -1) break;
	        
	        // Bounds check: ensure endTag is within page length
	        if (endTag >= page.length() || startTag + 1 >= page.length()) {
	            break;
	        }

	        String tag = page.substring(startTag + 1, endTag).trim(); // Original case
	        String tagLower = lower.substring(startTag + 1, endTag).trim(); // Lowercased

	        // Skip closing tags
	        if (tagLower.startsWith("/")) {
	            index = endTag + 1;
	            continue;
	        }

	        // Split tag contents into pieces
	        String[] parts = tagLower.split("\\s+");
	        if (parts.length == 0) {
	            index = endTag + 1;
	            continue;
	        }

	        String tagName = parts[0];
	        if (!tagName.equals("a")) {  // Only process <a ...>
	            index = endTag + 1;
	            continue;
	        }

	        // Find href= in the tag
	        int hrefIndex = tagLower.indexOf("href=");
	        if (hrefIndex == -1) {
	            index = endTag + 1;
	            continue;
	        }

	        // Extract the quoted URL value
	        int quoteStart = hrefIndex + 5;
	        while (quoteStart < tag.length() && Character.isWhitespace(tag.charAt(quoteStart))) quoteStart++;
	        if (quoteStart >= tag.length()) {
	            index = endTag + 1;
	            continue;
	        }

	        char quoteChar = tag.charAt(quoteStart);
	        int quoteEnd;
	        if (quoteChar == '"' || quoteChar == '\'') {
	            quoteEnd = tag.indexOf(quoteChar, quoteStart + 1);
	            if (quoteEnd != -1) {
	                String url = tag.substring(quoteStart + 1, quoteEnd).trim();
	                // Remove any trailing characters that shouldn't be in URL (> or other tag chars)
	                url = url.replaceAll("[>\"'\\s]+$", "");
	                if (!url.isEmpty()) {
	                    urls.add(url);
	                }
	            }
	        } else {
	            // href without quotes (e.g., href=http://foo.com)
	            // Find the end: space, >, or end of tag
	            int spaceEnd = tag.indexOf(' ', quoteStart);
	            int gtEnd = tag.indexOf('>', quoteStart);
	            int endPos = tag.length();
	            
	            if (spaceEnd != -1 && (gtEnd == -1 || spaceEnd < gtEnd)) {
	                endPos = spaceEnd;
	            } else if (gtEnd != -1) {
	                endPos = gtEnd;
	            }
	            
	            String url = tag.substring(quoteStart, endPos).trim();
	            // Remove any trailing characters that shouldn't be in URL (> or other tag chars)
	            url = url.replaceAll("[>\"'\\s]+$", "");
	            if (!url.isEmpty()) {
	                urls.add(url);
	            }
	        }

	        index = endTag + 1;
	    }

	    return urls;
	}
	
	public static List<String> normalizeUrls(List<String> urls, String baseURL) {
	    List<String> normalized = new ArrayList<>();

	    // Parse the base URL
	    String[] baseParts = URLParser.parseURL(baseURL);
	    String baseScheme = baseParts[0]; // e.g., "http"
	    String baseHost = baseParts[1];   // e.g., "foo.com"
	    String basePort = baseParts[2];   // e.g., "8000"
	    String basePath = baseParts[3];   // e.g., "/bar/xyz.html"

	    // If basePort is missing, add default
	    if (basePort == null || basePort.isEmpty()) {
	        basePort = baseScheme.equals("https") ? "443" : "80";
	    }

	    // Get the directory of the base path
	    int lastSlash = basePath.lastIndexOf('/');
	    String baseDir = (lastSlash >= 0) ? basePath.substring(0, lastSlash + 1) : "/";

	    for (String url : urls) {
	        if (url == null) continue;

	        // Remove fragment (#)
	        int hashIndex = url.indexOf('#');
	        if (hashIndex != -1) {
	            url = url.substring(0, hashIndex);
	        }
	        url = url.trim();
	        if (url.isEmpty()) continue;

	        // Filter out non-HTTP/HTTPS protocols
	        String lowerUrl = url.toLowerCase();
	        if (lowerUrl.startsWith("javascript:") || 
	            lowerUrl.startsWith("mailto:") || 
	            lowerUrl.startsWith("tel:") ||
	            lowerUrl.startsWith("ssh:") ||
	            lowerUrl.startsWith("ftp:") ||
	            lowerUrl.startsWith("file:")) {
	            continue; // Skip non-HTTP links
	        }

	        // Parse the URL
	        String[] parts = URLParser.parseURL(url);
	        String scheme = parts[0];
	        String host = parts[1];
	        String port = parts[2];
	        String path = parts[3];

	        // Handle relative URLs
	        if (host == null || host.isEmpty()) {
	            host = baseHost;
	            
	            if (path == null || path.isEmpty()) {
	                path = "/";
	            } else if (path.startsWith("/")) {
	                // absolute path on same host, fine
	            } else {
	                // relative path - resolve against base directory
	                // Handle "../" and "./" properly
	                String combinedPath = baseDir + path;
	                
	                // Normalize the path by resolving ".." and "."
	                String[] segments = combinedPath.split("/");
	                List<String> result = new ArrayList<>();
	                
	                for (String segment : segments) {
	                    if (segment.equals("..")) {
	                        // Go up one level
	                        if (!result.isEmpty() && !result.get(result.size() - 1).isEmpty()) {
	                            result.remove(result.size() - 1);
	                        }
	                    } else if (!segment.equals(".") && !segment.isEmpty()) {
	                        result.add(segment);
	                    }
	                }
	                
	                // Rebuild path
	                if (result.isEmpty() || (result.size() == 1 && result.get(0).isEmpty())) {
	                    path = "/";
	                } else {
	                    // Skip empty first element if present (from leading slash)
	                    int startIdx = (result.get(0).isEmpty() && result.size() > 1) ? 1 : 0;
	                    if (startIdx == 0) {
	                        path = "/" + String.join("/", result);
	                    } else {
	                        path = "/" + String.join("/", result.subList(startIdx, result.size()));
	                    }
	                }
	                
	                // Clean up any double slashes
	                path = path.replaceAll("//+", "/");
	            }
	        }

	        // Set scheme and port if missing
	        if (scheme == null || scheme.isEmpty()) {
	            scheme = baseScheme;
	        }
	        
	        // Filter out non-HTTP/HTTPS schemes
	        if (!scheme.equals("http") && !scheme.equals("https")) {
	            continue;
	        }
	        
	        if (port == null || port.isEmpty()) {
	            port = scheme.equals("https") ? "443" : "80";
	        }

	        // Filter out non-useful file extensions
	        if (path != null && !path.isEmpty()) {
	            String lowerPath = path.toLowerCase();
	            // Filter out common non-HTML file extensions
	            if (lowerPath.matches(".*\\.(jpg|jpeg|png|gif|bmp|svg|ico|webp|pdf|zip|tar|gz|rar|exe|dmg|mp3|mp4|avi|mov|wmv|flv|css|js|json|xml|txt|csv|doc|docx|xls|xlsx|ppt|pptx)(\\?.*)?$")) {
	                continue; // Skip non-HTML files
	            }
	        }

	        // Build final normalized URL
	        String normalizedUrl = scheme + "://" + host + ":" + port + path;

	        normalized.add(normalizedUrl);
	    }

	    return normalized;
	}
final static long RATE_LIMIT = 1000;
	
	public static boolean isAllowed(String url, String robots) {
	    if (robots == null || robots.isEmpty()) {
	        return true; // no robots.txt → allow everything
	    }

	    String hostRules = getRelevantSection(robots);
	    if (hostRules == null) return true; // no section for us

	    // Parse rules line by line
	    String[] lines = hostRules.split("\n");
	    String path;
	    try {
	        path = new URL(url).getPath(); // only compare paths
	    } catch (Exception e) {
	        return true;
	    }

	    // Store rules in the order they appear
	    List<String[]> rules = new ArrayList<>();
	    for (String raw : lines) {
	        String line = raw.trim();
	        if (line.isEmpty() || line.startsWith("#")) continue;
	        if (line.toLowerCase().startsWith("allow:")) {
	            String prefix = line.substring(6).trim();
	            rules.add(new String[]{"allow", prefix});
	        } else if (line.toLowerCase().startsWith("disallow:")) {
	            String prefix = line.substring(9).trim();
	            rules.add(new String[]{"disallow", prefix});
	        }
	    }

	    // Apply first matching rule
	    for (String[] rule : rules) {
	        String type = rule[0];
	        String prefix = rule[1];
	        if (prefix.isEmpty()) continue; // Disallow: (empty) means allow all
	        if (path.startsWith(prefix)) {
	            return type.equals("allow");
	        }
	    }

	    // Default: allow
	    return true;
	}

	// Helper to extract the relevant User-agent section
	private static String getRelevantSection(String robots) {
	    String[] lines = robots.split("\n");
	    StringBuilder section = new StringBuilder();
	    boolean inSection = false;
	    boolean foundSpecific = false;

	    for (String raw : lines) {
	        String line = raw.trim();
	        if (line.isEmpty() || line.startsWith("#")) continue;

	        if (line.toLowerCase().startsWith("user-agent:")) {
	            String agent = line.substring(11).trim().toLowerCase();
	            if (agent.equals("cis5550-crawler")) {
	                section.setLength(0);
	                inSection = true;
	                foundSpecific = true;
	                continue;
	            } else if (agent.equals("*") && !foundSpecific) {
	                section.setLength(0);
	                inSection = true;
	                continue;
	            } else {
	                inSection = false;
	                continue;
	            }
	        }

	        if (inSection) {
	            section.append(line).append("\n");
	        }
	    }

	    return section.length() > 0 ? section.toString() : null;
	}

	// Crawl delay extraction
	public static float crawlDelay(String url, String robots) {
	    if (robots == null || robots.isEmpty()) {
	        return 1.0f; // default 1 second if missing
	    }

	    String hostRules = getRelevantSection(robots);
	    if (hostRules == null) return 1.0f;

	    String[] lines = hostRules.split("\n");
	    for (String raw : lines) {
	        String line = raw.trim().toLowerCase();
	        if (line.startsWith("crawl-delay:")) {
	            try {
	                return Float.parseFloat(line.substring(12).trim());
	            } catch (Exception e) {
	                return 1.0f;
	            }
	        }
	    }

	    return 1.0f; // default
	}


	
	public static void run(FlameContext context, String[] arr) {
	    if (arr == null || arr.length < 1) {
	        context.output("ERROR");
	        return;
	    }

	    context.output("OK");
	    System.out.println("hi new version");

	    String seedURL = arr[0];
	    //String normalizedSeed = normalizeUrls(Arrays.asList(seedURL), seedURL).get(0);

	    FlameRDD urlQueue;
		try {
			urlQueue = context.parallelize(List.of(normalizeUrls(List.of(seedURL), seedURL).get(0)));
		    

		    while (urlQueue.count() > 0) {
		    	try {
		    		// Get KVS coordinator address before flatMap to avoid serialization issues
		    		String kvsCoordinator = context.getKVS().getCoordinator();
		    		
					urlQueue = urlQueue.flatMap( url ->{
						List<String> out = new ArrayList<>();
						try {
							String host = new URL(url).getHost();
					        KVSClient kvs = new KVSClient(kvsCoordinator);
					        long now = System.currentTimeMillis();

					        // Fetch or create host row
					        Row hostRow;
					        if (kvs.existsRow("hosts", host)) {
					            hostRow = kvs.getRow("hosts", host);
					        } else {
					            hostRow = new Row(host);
					            kvs.putRow("hosts", hostRow);
					        }

					        // Fetch robots.txt if not already fetched
					        String robots = hostRow.get("robotsTxt");
					        if (robots == null) {
					            try {
					                HttpURLConnection rconn = (HttpURLConnection) new URL("http://" + host + "/robots.txt").openConnection();
					                rconn.setRequestMethod("GET");
					                rconn.setRequestProperty("User-Agent", "cis5550-crawler");
					                rconn.connect();
					                if (rconn.getResponseCode() == 200) {
					                    robots = new String(rconn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
					                } else {
					                    robots = "";
					                }
					            } catch (Exception e) {
					                robots = "";
					            }
					            hostRow.put("robotsTxt", robots);
					            kvs.putRow("hosts", hostRow);
					        }

					        // Check if URL is allowed
					        if (!isAllowed(url, robots)) {
					            return out; // skip disallowed URL
					        }
					        
					        float delay = crawlDelay(url, robots);
					        long delayMillis = (long) (delay * 1000);

					        // Rate limiting
					        long lastAccess = 0;
					        String ts = hostRow.get("lastAccess");
					        if (ts != null) lastAccess = Long.parseLong(ts);
					        long RATE_LIMIT = 1000; // milliseconds
					        if (now - lastAccess < RATE_LIMIT) {
					        	out.add(url);
					        	return out;
					        }

							// Otherwise, update timestamp
					        hostRow.put("lastAccess", Long.toString(System.currentTimeMillis()));
					        kvs.putRow("hosts", hostRow);



					        String urlHash = Integer.toString(url.hashCode());

					        // Skip if already in KVS (check both pt-crawl and visited tables)
					        if (kvs.existsRow("pt-crawl", urlHash) || kvs.existsRow("visited", urlHash)) {
					            return out; // already visited (either crawled or skipped)
					        }
						
					        // Create row for potential storage in pt-crawl (only for useful pages)
					        Row r = new Row(urlHash);
					        r.put("url", url);
					        
					        // mark as seen
					        Thread.sleep(delayMillis);
					        
					        // add head requests
					        HttpURLConnection hconn = (HttpURLConnection) new URL(url).openConnection();
					        hconn.setRequestMethod("HEAD");
					        hconn.setInstanceFollowRedirects(false);
					        hconn.setRequestProperty("User-Agent", "cis5550-crawler");
					        hconn.connect();
					        int hcode = hconn.getResponseCode();
					        String contentType = hconn.getContentType();
					        int contentLengthInt = hconn.getContentLength();
					        String contentLength = contentLengthInt != -1 ? Integer.toString(contentLengthInt) : null;
					        
					        if (contentType != null) {
					        	r.put("contentType", contentType); 
					        }
					        
					        if (contentLength != null) {
					        	r.put("length", contentLength);
					        }
					        
					        // Check for redirects BEFORE storing in pt-crawl
					        if (hcode >= 300 && hcode < 400) {
					            // It's a redirect - store in "visited" table, not pt-crawl
					            Row visitedRow = new Row(urlHash);
					            visitedRow.put("url", url);
					            visitedRow.put("responseCode", Integer.toString(hcode));
					            if (contentLength != null) {
					                visitedRow.put("length", contentLength);
					            }
					            if (contentType != null) {
					                visitedRow.put("contentType", contentType);
					            }
					            kvs.putRow("visited", visitedRow);
					            
					            // Queue the redirect destination URL
					            String newUrl = hconn.getHeaderField("Location");
					            if (newUrl != null && !newUrl.isEmpty()) {
					            	List<String> normalized = normalizeUrls(List.of(newUrl), url);
					                out.addAll(normalized);
					            }
					            return out; // don't GET this URL now
					        }
					        
					        // Check for non-200 responses BEFORE storing in pt-crawl
					        if (hcode != 200) {
					            // Non-200 response - store in "visited" table, not pt-crawl
					            Row visitedRow = new Row(urlHash);
					            visitedRow.put("url", url);
					            visitedRow.put("responseCode", Integer.toString(hcode));
					            if (contentLength != null) {
					                visitedRow.put("length", contentLength);
					            }
					            if (contentType != null) {
					                visitedRow.put("contentType", contentType);
					            }
					            kvs.putRow("visited", visitedRow);
					        	return out;
					        }
						
						// Only store 200 OK responses in pt-crawl (will be updated later with page content if long enough)
						r.put("responseCode", Integer.toString(hcode));
						kvs.putRow("pt-crawl", r);
					        
					        // Filter out short pages using Content-Length from HEAD request
					        // Minimum length: 5000 bytes to avoid citation notes and stubs while preserving real articles
					        if (contentLengthInt != -1 && contentLengthInt < 5000) {
					            System.out.println("DEBUG: Skipping short page (" + contentLengthInt + " bytes): " + url);
					            // Store in "visited" table (not pt-crawl) to mark as visited without storing content
					            Row visitedRow = new Row(urlHash);
					            visitedRow.put("url", url);
					            visitedRow.put("responseCode", Integer.toString(hcode));
					            visitedRow.put("length", contentLength);
					            if (contentType != null) {
					                visitedRow.put("contentType", contentType);
					            }
					            kvs.putRow("visited", visitedRow);
					            return out; // Don't GET short pages
					        }
					        
					        if (hcode == 200 && contentType != null && contentType.toLowerCase().startsWith("text/html")) {
					            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
					            conn.setRequestMethod("GET");
					            conn.setRequestProperty("User-Agent", "cis5550-crawler");
					            conn.connect();
					            int code = conn.getResponseCode(); // overwrite
					            
						        if (code >= 300 && code < 400) {
						            // Redirect - store in pt-crawl
						            r.put("responseCode", Integer.toString(code));
						            kvs.putRow("pt-crawl", r);
						            return out;
						        }
						        
						        if (code == 200) {
						            String page = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
							
						            // Check page length as fallback (in case Content-Length header was missing from HEAD)
						            // Minimum length: 5000 bytes to avoid citation notes and stubs
						            if (page.length() < 5000) {
						                System.out.println("DEBUG: Skipping short page (" + page.length() + " chars): " + url);
						                // Store in "visited" table (not pt-crawl) to mark as visited without storing content
						                Row visitedRow = new Row(urlHash);
						                visitedRow.put("url", url);
						                visitedRow.put("responseCode", Integer.toString(code));
						                visitedRow.put("length", Integer.toString(page.length()));
						                String contentTypeFromGet = conn.getContentType();
						                if (contentTypeFromGet != null) {
						                    visitedRow.put("contentType", contentTypeFromGet);
						                }
						                kvs.putRow("visited", visitedRow);
						                return out; // Don't extract links from short pages
						            }
							
						            // Page is long enough - store in pt-crawl
						            r.put("responseCode", Integer.toString(code));

						            // Store in KVS with page
						            r.put("page", page);
						            r.put("length", Integer.toString(page.length()));
						            kvs.putRow("pt-crawl", r);


						            // Extract links and normalize
						            List<String> rawLinks = extractedUrls(page);
						            List<String> links = normalizeUrls(rawLinks, url);
							
						            // Debug output
						            System.out.println("DEBUG: Extracted " + rawLinks.size() + " raw links, normalized to " + links.size() + " links from " + url);

						            // Add new links to the queue if not visited
						            int addedCount = 0;
						            int skippedCount = 0;
						            for (String l : links) {
						            	if (l.contains("/nocrawl/")) {
						            	    skippedCount++;
						            	    continue;
						            	}
						            	String linkHash = Integer.toString(l.hashCode());
						            	if (!kvs.existsRow("pt-crawl", linkHash)) {
						            		out.add(l);
						            		addedCount++;
						            	} else {
						            		skippedCount++;
						            	}
						            }
						            System.out.println("DEBUG: Added " + addedCount + " new links, skipped " + skippedCount + " (already visited or nocrawl)");
						            return out;
						        } 
					        } else {
					        	kvs.putRow("pt-crawl", r);
					        	return out;
					        }
					      
					        Thread.sleep(50);
					    } catch (Exception e) {
					        System.err.println("Error fetching " + url + ": " + e.getMessage());
					    }
						return out;
					}).distinct();
					
					// Shuffle the queue to avoid alphabetical processing and improve domain diversity
					urlQueue = urlQueue.distinct();

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	
		        
		    }
		    context.output("OK");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    

	}

}
