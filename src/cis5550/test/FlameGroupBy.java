package cis5550.test;

import cis5550.flame.*;
import java.util.*;

public class FlameGroupBy {
	public static void run(FlameContext ctx, String args[]) throws Exception {
		List<String> list = Arrays.asList("pink", "blue", "pink", "green", "yellow", "orange", "pink", "green", "blue", "yellow", "pink", "red");
		
		FlameRDD rdd = ctx.parallelize(list);
		
		// key = string itself
		FlamePairRDD grouped = ((FlameRDDImpl) rdd).groupBy(s -> s);
		
		List<FlamePair> out = grouped.collect();
		Collections.sort(out);
		
		StringBuilder sb = new StringBuilder();
		for (FlamePair pair : out) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(pair.toString());
		}
		ctx.output(sb.toString());
	}
}
