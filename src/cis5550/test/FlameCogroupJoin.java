package cis5550.test;

import cis5550.flame.*;
import java.util.*;


public class FlameCogroupJoin {
	public static void run(FlameContext ctx, String args[]) throws Exception {
		// Create first PairRDD with some test data
		// Format: "key1 value1", "key2 value2", etc.
		LinkedList<String> list1 = new LinkedList<String>();
		for (int i = 0; i < args.length / 2; i++) {
			list1.add(args[i]);
		}
		
		// Create second PairRDD with some test data
		LinkedList<String> list2 = new LinkedList<String>();
		for (int i = args.length / 2; i < args.length; i++) {
			list2.add(args[i]);
		}
		
		// Parse and create PairRDDs
		FlamePairRDD rdd1 = ctx.parallelize(list1)
			.mapToPair(s -> {
				String[] pieces = s.split(" ");
				if (pieces.length >= 2) {
					return new FlamePair(pieces[0], pieces[1]);
				}
				return new FlamePair(s, "");
			});
		
		FlamePairRDD rdd2 = ctx.parallelize(list2)
			.mapToPair(s -> {
				String[] pieces = s.split(" ");
				if (pieces.length >= 2) {
					return new FlamePair(pieces[0], pieces[1]);
				}
				return new FlamePair(s, "");
			});
		
		// Perform cogroupJoin
		FlamePairRDD result = rdd1.cogroupJoin(rdd2);
		
		// Collect and sort results
		List<FlamePair> out = result.collect();
		Collections.sort(out);
		
		// Format output
		String resultStr = "";
		for (FlamePair p : out) {
			resultStr = resultStr + (resultStr.equals("") ? "" : ",") + p.toString();
		}
		
		ctx.output(resultStr);
	}
}

