package cis5550.test;

import cis5550.flame.*;
import java.util.*;

public class FlameIntersection {
	public static void run(FlameContext ctx, String args[]) throws Exception {
		List<String> left = Arrays.asList("pink", "pink", "blue", "red", "yellow", "blue", "yellow", "green", "pink");
		List<String> right = Arrays.asList("pink", "pink", "yellow", "orange", "blue", "red", "blue");
		
		FlameRDD rddL = ctx.parallelize(left);
		FlameRDD rddR = ctx.parallelize(right);
		
		FlameRDD intersection = rddL.intersection(rddR);
		
		List<String> out = intersection.collect();
		Collections.sort(out);
		
		String result = "";
		for (String s : out)
			result = result+(result.equals("") ? "" : ",")+s;
		
		ctx.output(result);
	}
}