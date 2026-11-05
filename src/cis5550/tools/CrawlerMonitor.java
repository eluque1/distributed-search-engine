package cis5550.tools;

import cis5550.kvs.KVSClient;

public class CrawlerMonitor {
    private static final int TARGET_PAGES = 5500;
    private static final int TARGET_DOMAINS = 275;
    private static final int CHECK_INTERVAL_SECONDS = 5; 
    
    public static void main(String[] args) throws Exception {
        String kvsCoordinator = "localhost:8000";
        boolean autoStop = false;
        
        // Parse arguments
        if (args.length > 0) {
            kvsCoordinator = args[0];
        }
        if (args.length > 1 && args[1].equals("--auto-stop")) {
            autoStop = true;
            System.out.println("Auto-stop enabled: Will stop crawler at " + TARGET_PAGES + 
                             " pages or " + TARGET_DOMAINS + " domains");
        }
        
        KVSClient kvs = new KVSClient(kvsCoordinator);
        
        System.out.println("=== Crawler Speed Monitor ===");
        if (autoStop) {
            System.out.println("Target: " + TARGET_PAGES + " pages (auto-stop enabled)");
        } else {
            System.out.println("Running indefinitely (no target)");
        }
        System.out.println("Checking every " + CHECK_INTERVAL_SECONDS + " seconds...\n");
        System.out.println(String.format("%-8s | %-10s | %-12s | %-15s", 
            "Time", "Total Pages", "Current Rate", "Avg Rate"));
        System.out.println("---------|------------|--------------|----------------");
        
        int previousPages = 0;
        long previousTime = System.currentTimeMillis();
        long startTime = previousTime;
        int startPages = 0;
        boolean firstRun = true;
        
        while (true) {
            try {
                long currentTime = System.currentTimeMillis();
                
                // Fast count using KVS count API (no scanning!)
                int totalPages = kvs.count("pt-crawl");
                
                if (firstRun) {
                    startPages = totalPages;
                    previousPages = totalPages;
                    firstRun = false;
                }
                
                // Calculate speed
                long elapsedMs = currentTime - previousTime;
                double elapsedSec = elapsedMs / 1000.0;
                int pagesDelta = totalPages - previousPages;
                double currentSpeed = (elapsedSec > 0) ? pagesDelta / elapsedSec : 0;
                
                // Calculate overall average speed
                long totalElapsedMs = currentTime - startTime;
                double totalElapsedSec = totalElapsedMs / 1000.0;
                int totalPagesCrawled = totalPages - startPages;
                double avgSpeed = (totalElapsedSec > 0) ? totalPagesCrawled / totalElapsedSec : 0;
                
                // Print status (compact format) - no target/progress bar
                System.out.println(String.format("%-8s | %,10d | %8.2f p/s | %8.2f p/s",
                    formatTime(totalElapsedSec),
                    totalPages,
                    currentSpeed,
                    avgSpeed));
                
                // Update for next iteration
                previousPages = totalPages;
                previousTime = currentTime;
                
                // Check if thresholds are met
                if (autoStop && totalPages >= TARGET_PAGES) {
                    System.out.println("\n✓ Target reached! Final stats:");
                    System.out.println("  Total Pages: " + totalPages);
                    System.out.println("  Total Time: " + formatTime(totalElapsedSec));
                    System.out.println("  Average Speed: " + String.format("%.2f pages/sec", avgSpeed));
                    stopCrawler();
                    break;
                }
                
                // Sleep before next check
                Thread.sleep(CHECK_INTERVAL_SECONDS * 1000);
                
            } catch (Exception e) {
                System.err.println("\nError during monitoring: " + e.getMessage());
                Thread.sleep(CHECK_INTERVAL_SECONDS * 1000);
            }
        }
    }
    
    private static String formatTime(double seconds) {
        int hrs = (int)(seconds / 3600);
        int mins = (int)((seconds % 3600) / 60);
        int secs = (int)(seconds % 60);
        if (hrs > 0) {
            return String.format("%d:%02d:%02d", hrs, mins, secs);
        } else {
            return String.format("%d:%02d", mins, secs);
        }
    }
    
    private static String createProgressBar(double percent, int width) {
        int filled = (int)((percent / 100.0) * width);
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < width; i++) {
            if (i < filled) {
                bar.append("█");
            } else {
                bar.append("░");
            }
        }
        bar.append("]");
        return bar.toString();
    }
    
    private static void stopCrawler() {
        try {
            // find and kill the flame coord lol this might not work
            String os = System.getProperty("os.name").toLowerCase();
            
            if (os.contains("win")) {
                
                ProcessBuilder pb = new ProcessBuilder("tasklist", "/FI", "WINDOWTITLE eq *Coordinator*");
                Process p = pb.start();

                System.out.println("Please manually stop the Flame Coordinator process (Ctrl+C in its terminal)");
            } else {
                ProcessBuilder pb = new ProcessBuilder("pkill", "-f", "cis5550.flame.Coordinator");
                Process p = pb.start();
                p.waitFor();
                System.out.println("Attempted to stop Flame Coordinator");
            }
        } catch (Exception e) {
            System.err.println("Could not automatically stop crawler: " + e.getMessage());
            System.out.println("Please manually stop the Flame Coordinator (Ctrl+C in its terminal)");
        }
    }
}