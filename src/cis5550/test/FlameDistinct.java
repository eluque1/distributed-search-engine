package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import java.util.*;

public class FlameDistinct {
    public static void run(FlameContext ctx, String[] args) {
        try {
            // Create RDD with duplicates
            List<String> data = Arrays.asList(
                "apple", "banana", "apple", "cherry", 
                "banana", "date", "apple", "elderberry"
            );
            
            FlameRDD rdd = ctx.parallelize(data);
            
            // Test distinct
            FlameRDD distinctRDD = rdd.distinct();
            
            // Collect and sort results
            List<String> results = distinctRDD.collect();
            Collections.sort(results);
            
            // Output results
            String output = String.join(",", results);
            ctx.output(output);
            
        } catch (Exception e) {
            ctx.output("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}