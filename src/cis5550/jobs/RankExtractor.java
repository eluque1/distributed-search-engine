package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.util.Iterator;

public class RankExtractor {

    public static void run(FlameContext ctx, String[] args) {
        // Default to iter-15 if no argument is provided
        String sourceTable = (args.length > 0) ? args[0] : "pt-graph-iter-15";
        String destTable = "pt-pageranks";

        ctx.output("Starting Rank Extraction...\n");
        ctx.output("Source: " + sourceTable + "\n");
        ctx.output("Dest:   " + destTable + "\n");

        try {
            KVSClient kvs = ctx.getKVS();

            // Check if source exists
            if (!kvs.scan(sourceTable).hasNext()) {
                ctx.output("ERROR: Source table '" + sourceTable + "' is empty or does not exist!\n");
                return;
            }

            // Optional: Clear destination table first to avoid mixing old data
            // kvs.deleteTable(destTable); 

            Iterator<Row> iter = kvs.scan(sourceTable);
            int count = 0;

            while (iter.hasNext()) {
                Row r = iter.next();
                
                // 1. Get Key (Clean it just in case)
                String key = r.key().trim();
                
                // 2. Extract Rank (First column in value string)
                // Format: currentRank;oldRank;outDegree;links...
                String val = r.get("value");
                if (val != null) {
                    String[] parts = val.split(";");
                    if (parts.length > 0) {
                        String rank = parts[0];
                        
                        // 3. Write to pt-pageranks
                        Row prRow = new Row(key);
                        prRow.put("rank", rank);
                        kvs.putRow(destTable, prRow);
                        
                        count++;
                        if (count % 5000 == 0) {
                            ctx.output("Extracted " + count + " ranks...\n");
                        }
                    }
                }
            }

            ctx.output("DONE! Extracted " + count + " ranks to '" + destTable + "'.\n");

        } catch (Exception e) {
            e.printStackTrace();
            ctx.output("Extraction Failed: " + e.getMessage() + "\n");
        }
    }
}