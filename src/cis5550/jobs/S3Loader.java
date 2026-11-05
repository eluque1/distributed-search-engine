package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlamePair;
import cis5550.kvs.Row;
import java.util.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Simple S3 loader that downloads files from S3 and writes them to pt-crawl KVS table.
 * Uses AWS CLI (no SDK dependencies needed).
 * 
 * HOW IT WORKS:
 * 1. Master runs "aws s3 ls" to list all files in S3 folders
 * 2. Files are hash-partitioned across N workers
 * 3. Each worker downloads its files using "aws s3 cp" and writes to local KVS
 * 4. After completion, run your existing Indexer.java
 * 
 * WORKER SCALING:
 * - Can use as many workers as you have EC2 instances
 * - Each worker gets ~(total files / num workers) files
 * - More workers = faster parallel download
 * - Recommended: 10-20 workers for 233K files (~11K-23K files per worker)
 * 
 * USAGE:
 * ./flame-submit.sh S3Loader 555-project-crawl 15
 */
public class S3Loader {
    
    private static final String[] S3_FOLDERS = {
        "pt-crawl", 
        "pt-crawl-as2", 
        "pt-crawl-as3", 
        "pt-crawl-as1"
    };
    
    /**
     * List all S3 keys using AWS CLI.
     */
    private static List<String> listAllS3Keys(String bucketName) throws Exception {
        List<String> allKeys = new ArrayList<>();
        
        for (String folder : S3_FOLDERS) {
            System.out.println("Listing S3 keys in folder: " + folder);
            
            // Run: aws s3 ls s3://bucket/folder/ --recursive
            ProcessBuilder pb = new ProcessBuilder(
                "aws", "s3", "ls", 
                "s3://" + bucketName + "/" + folder + "/", 
                "--recursive"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();
            
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8)
            );
            
            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                // Parse: "2024-12-10 12:34:56   1234 pt-crawl/abc123"
                // Split on whitespace, key is the last part
                String[] parts = line.split("\\s+");
                if (parts.length >= 4) {
                    String key = parts[parts.length - 1]; // Last part is the key
                    if (!key.endsWith("/")) {
                        allKeys.add(key);
                        lineCount++;
                    }
                }
            }
            
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                System.err.println("Warning: AWS CLI returned exit code " + exitCode + " for folder " + folder);
            }
            
            System.out.println("Folder " + folder + " complete: found " + lineCount + " files. Total so far: " + allKeys.size());
        }
        
        return allKeys;
    }
    
    /**
     * Download content from S3 using AWS CLI.
     */
    private static String downloadFromS3(String bucketName, String key) throws Exception {
        // Run: aws s3 cp s3://bucket/key -
        ProcessBuilder pb = new ProcessBuilder(
            "aws", "s3", "cp",
            "s3://" + bucketName + "/" + key,
            "-" // Output to stdout
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        
        // Read content from stdout
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int bytesRead;
        InputStream is = p.getInputStream();
        while ((bytesRead = is.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new IOException("AWS CLI failed with exit code " + exitCode + " for key: " + key);
        }
        
        return baos.toString(StandardCharsets.UTF_8.name());
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length < 2) {
            ctx.output("Usage: S3Loader <bucket-name> <num-workers>\n");
            ctx.output("Example: S3Loader 555-project-crawl 15\n");
            return;
        }
        
        String bucketName = args[0];
        int numWorkers = Integer.parseInt(args[1]);
        
        ctx.output("=== S3 LOADER STARTED ===\n");
        ctx.output("Bucket: " + bucketName + "\n");
        ctx.output("Target workers: " + numWorkers + "\n\n");
        
        ctx.output("WORKFLOW EXPLANATION:\n");
        ctx.output("1. Master lists all S3 files (using aws s3 ls)\n");
        ctx.output("2. Files are hash-partitioned across workers\n");
        ctx.output("3. Each worker downloads its assigned files in parallel\n");
        ctx.output("4. Workers write to their local pt-crawl KVS partition\n");
        ctx.output("5. After completion, run Indexer to process pt-crawl\n\n");
        
        // STEP 1: Master lists all S3 keys
        ctx.output("STEP 1: Listing all S3 files...\n");
        ctx.output("Folders: " + String.join(", ", S3_FOLDERS) + "\n");
        long startList = System.currentTimeMillis();
        List<String> allKeys = listAllS3Keys(bucketName);
        long endList = System.currentTimeMillis();
        ctx.output("✓ Found " + allKeys.size() + " files in " + (endList - startList) + "ms\n\n");
        
        if (allKeys.isEmpty()) {
            ctx.output("No files found. Exiting.\n");
            return;
        }
        
        // STEP 2: Create worker assignments (hash-based partitioning)
        ctx.output("STEP 2: Creating worker assignments (hash partitioning)...\n");
        List<String> workerAssignments = new ArrayList<>();
        
        // Get KVS coordinator address to pass to workers
        String kvsCoordinator = ctx.getKVS().getCoordinator();
        
        // Count files per worker for reporting
        int[] fileCounts = new int[numWorkers];
        for (String key : allKeys) {
            int workerId = Math.abs(key.hashCode()) % numWorkers;
            fileCounts[workerId]++;
        }
        
        for (int workerId = 0; workerId < numWorkers; workerId++) {
            StringBuilder assignment = new StringBuilder();
            assignment.append(workerId).append("|");
            assignment.append(kvsCoordinator).append("|");
            assignment.append(bucketName).append(":");
            
            for (String key : allKeys) {
                if (Math.abs(key.hashCode()) % numWorkers == workerId) {
                    assignment.append(key).append(",");
                }
            }
            
            String assignmentStr = assignment.toString();
            if (assignmentStr.endsWith(",")) {
                assignmentStr = assignmentStr.substring(0, assignmentStr.length() - 1);
            }
            workerAssignments.add(assignmentStr);
            ctx.output("  Worker " + workerId + ": " + fileCounts[workerId] + " files\n");
        }
        ctx.output("\n");
        
        // STEP 3: Each worker downloads its files and writes to KVS
        ctx.output("STEP 3: Workers downloading and writing to pt-crawl...\n");
        ctx.output("(This may take 10-20 minutes depending on file sizes)\n");
        long startDownload = System.currentTimeMillis();
        
        FlameRDD assignmentsRDD = ctx.parallelize(workerAssignments);
        
        FlameRDD results = assignmentsRDD.flatMap(assignment -> {
            List<String> result = new ArrayList<>();
            
            // Parse assignment: "workerId|kvsCoordinator|bucketName:key1,key2,key3,..."
            int colonIdx = assignment.indexOf(':');
            if (colonIdx == -1) {
                result.add("error:Invalid assignment format");
                return result;
            }
            
            String header = assignment.substring(0, colonIdx);
            String[] headerParts = header.split("\\|");
            if (headerParts.length != 3) {
                result.add("error:Invalid header format");
                return result;
            }
            
            String workerId = headerParts[0];
            String workerKvsCoord = headerParts[1];
            String workerBucket = headerParts[2];
            String keysStr = assignment.substring(colonIdx + 1);
            
            if (keysStr.isEmpty()) {
                result.add(workerId + ":0:0");
                return result;
            }
            
            String[] keys = keysStr.split(",");
            System.out.println("Worker " + workerId + " processing " + keys.length + " files");
            
            int processed = 0;
            int errors = 0;
            
            for (String key : keys) {
                try {
                    // Download from S3
                    String content = downloadFromS3(workerBucket, key);
                    
                    // Extract document ID from key (e.g., pt-crawl/abc123 -> abc123)
                    String docId = key.substring(key.lastIndexOf('/') + 1);
                    
                    // Create URL from key
                    String url = "http://crawled/" + docId;
                    
                    // Write to KVS pt-crawl table
                    Row row = new Row(docId);
                    row.put("url", url);
                    row.put("page", content);
                    
                    // Connect to KVS coordinator (not localhost)
                    cis5550.kvs.KVSClient kvs = new cis5550.kvs.KVSClient(workerKvsCoord);
                    kvs.putRow("pt-crawl", row);
                    
                    processed++;
                    if (processed % 500 == 0) {
                        System.out.println("Worker " + workerId + " progress: " + processed + "/" + keys.length);
                    }
                    
                } catch (Exception e) {
                    System.err.println("Worker " + workerId + " ERROR on key '" + key + "': " + e.getClass().getName() + " - " + e.getMessage());
                    e.printStackTrace();
                    errors++;
                }
            }
            
            System.out.println("Worker " + workerId + " COMPLETED: " + processed + " files, " + errors + " errors");
            result.add(workerId + ":" + processed + ":" + errors);
            return result;
        });
        
        // Collect results to trigger execution
        List<String> resultList = results.collect();
        
        long endDownload = System.currentTimeMillis();
        long downloadSeconds = (endDownload - startDownload) / 1000;
        
        ctx.output("\n=== S3 LOADER COMPLETED ===\n");
        ctx.output("Total files: " + allKeys.size() + "\n");
        ctx.output("Download time: " + downloadSeconds + " seconds (" + (downloadSeconds/60) + " minutes)\n\n");
        
        // Parse and display results
        int totalProcessed = 0;
        int totalErrors = 0;
        ctx.output("Worker Results:\n");
        for (String res : resultList) {
            String[] parts = res.split(":");
            if (parts.length == 3) {
                int workerId = Integer.parseInt(parts[0]);
                int processed = Integer.parseInt(parts[1]);
                int errors = Integer.parseInt(parts[2]);
                totalProcessed += processed;
                totalErrors += errors;
                ctx.output("  Worker " + workerId + ": " + processed + " processed, " + errors + " errors\n");
            }
        }
        
        ctx.output("\nTotal processed: " + totalProcessed + "\n");
        ctx.output("Total errors: " + totalErrors + "\n\n");
        ctx.output("✓ Data loaded into pt-crawl table!\n\n");
        ctx.output("NEXT STEP: Run indexer to process the data:\n");
        ctx.output("  ./flame-submit.sh Indexer\n");
    }
}
