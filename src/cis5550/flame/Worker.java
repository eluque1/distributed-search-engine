package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
	    if (args.length != 2) {
	    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
	    	System.exit(1);
	    }
	
	    int port = Integer.parseInt(args[0]);
	    String server = args[1];
		System.setProperty("FLAME_WORKER_ID", "worker-" + port);
		  startPingThread(server, ""+port, port);
	    final File myJAR = new File("__worker"+port+"-current.jar");
	
	  	port(port);
	
	    post("/useJAR", (request,response) -> {
	      FileOutputStream fos = new FileOutputStream(myJAR);
	      fos.write(request.bodyAsBytes());
	      fos.close();
	      return "OK";
	    });

	    post("/rdd/flatMap", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	Object l = Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	// get rows 
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	if (l instanceof StringToIterable) {
	    		StringToIterable lambda = (StringToIterable) l;
	    		
	    		// using op method invoke lambda on the value column in each row
		    	while(iter.hasNext()) {
		    		Row row = iter.next();
		    		String value = row.get("value");
		    		if (value != null) {
		    			Iterable<String> outputs = lambda.op(value);
		    			if (outputs != null) {
		    				for (String output : outputs) {
		    					if (output != null) {
			    					// use UUID to avoid overwriting rows
			    					String rowKey = UUID.randomUUID().toString();
			    					kvsC.put(outputTable, rowKey, "value", output);
		    					}
		    				}
		    			}
		    		}
		    	}
	    	} else {
	    		PairToStringIterable lambda = (PairToStringIterable) l;
	    		
	    		while(iter.hasNext()) {
	    			Row row = iter.next();
	    			for (String col : row.columns()) {
	    				String value = row.get(col);
	    				if (value != null) {
	    					FlamePair pair = new FlamePair(row.key(), value);
	    					Iterable<String> outputs = lambda.op(pair);
	    					if (outputs != null) {
	    						for (String s : outputs) {
	    							if (s != null) {
	    								String rowKey = UUID.randomUUID().toString();
	    								kvsC.put(outputTable, rowKey, "value", s);
	    							}
	    						}
	    					}
	    				}
	    			}
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	
	    post("/rdd/mapToPair", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	// hand the string from the input table to the lambda and get back a pair (k, v)
	    	while(iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    		if (value != null) {
	    			FlamePair pair = lambda.op(value);
	    			if (pair != null) {
	    		    	// v goes in a row k in the output table, with a unique column name
	    				String k = pair._1();
	    				String v = pair._2();
	    				String colName = row.key();
	    				kvsC.put(outputTable, k, colName, v);
	    			}
	    		}
	    	}
	    	
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/foldByKey", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	String zeroElement = req.queryParams("zeroElement");
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	while (iter.hasNext()) {
	    		Row row = iter.next();
	    		String accumulator = zeroElement;
	    		Set<String> columns = row.columns();
	    		for (String col : columns) {
	    			accumulator = lambda.op(accumulator, row.get(col));
	    		}
	    		kvsC.put(outputTable, row.key(), "value", accumulator);
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/deduplicate", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("originalTable");
	    	String outputTable = req.queryParams("noDuplicatesTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	while (iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    		if (value != null) {
	    			// use value as key, so it's unique, duplicates overwrite the same rowkey
	    			kvsC.put(outputTable, value, "value", value);
	    		}
	    	}

	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/intersection", (req, res) -> {
	    	// decode query params
	    	String leftTable = req.queryParams("noDuplicatesTable");
	    	String rightTable = req.queryParams("otherTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(rightTable, startKey, endKey);
	    	
	    	while (iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    		if (value != null) {
		    		// for each value in right, if it's in the left, add it to output
	    			if (kvsC.get(leftTable, value, "value") != null) {
	    				kvsC.put(outputTable, value, "value", value);
	    			}
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/groupBy", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	while(iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/fromTable", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");

	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	RowToString lambda = (RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	while (iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = lambda.op(row);
	    		if (value != null) {
		    		// add value to table if lambda op on row is not null
	    			kvsC.put(outputTable, row.key(), "value", value);
	    		}
	    	}	
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/flatMapToPair", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// get rows 
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	byte[] bodyBytes = req.bodyAsBytes();
	    	if (bodyBytes == null || bodyBytes.length == 0) {
	    		res.status(400, "Bad Request");
	    		return "ERROR: Request body is empty - lambda is required for flatMapToPair";
	    	}
	    	Object l = Serializer.byteArrayToObject(bodyBytes, myJAR);
	    	
	    	// if called by FlameRDD
	    	if (l instanceof StringToPairIterable) {
	    		StringToPairIterable lambda = (StringToPairIterable) l;
		    	
		    	// using op method invoke lambda on the value column in each row
		    	while(iter.hasNext()) {
		    		Row row = iter.next();
		    		String value = row.get("value");
		    		if (value != null) {
		    			Iterable<FlamePair> pairs = lambda.op(value);
		    			if (pairs != null) {
		    				for (FlamePair pair : pairs) {
		    					if (pair != null) {
			    					String k = pair._1();
			    					String v = pair._2();
			    					String colName = UUID.randomUUID().toString();
			    					kvsC.put(outputTable, k, colName, v);
		    					}
		    				}
		    			}
		    		}
		    	}
	    	}
	    	
	    	// else if called by FlamePairRDD
	    	else {
	    		PairToPairIterable lambda = (PairToPairIterable) l;
	    		
	    		while (iter.hasNext()) {
	    			Row row = iter.next();
	    			for (String col : row.columns()) {
	    				String value = row.get(col);
	    				if (value != null) {
	    					FlamePair pair = new FlamePair(row.key(), value);
	    					Iterable<FlamePair> pairs =  lambda.op(pair);
	    					if (pairs != null) {
	    						for (FlamePair p : pairs) {
	    							if (p != null) {
	    								String k = p._1();
	    								String v = p._2();
	    								String colName = UUID.randomUUID().toString();
	    								kvsC.put(outputTable, k, colName, v);
	    							}
	    						}
	    					}
	    				}
	    			}
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    
	    post("/rdd/join", (req, res) -> {
	    	// decode query params
	    	String tableOne = req.queryParams("tableOne");
	    	String tableTwo = req.queryParams("tableTwo");
	    	String output = req. queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(tableOne, startKey, endKey);
	    	
	    	while(iter.hasNext()) {
	    		Row row1 = iter.next();
	    		Row row2 = kvsC.getRow(tableTwo, row1.key());
	    		
	    		if (row2 != null) {
	    			for (String col1 : row1.columns()) {
	    				String value1 = row1.get(col1);
	    				if (value1 != null) {
	    					for (String col2 : row2.columns()) {
	    						String value2 = row2.get(col2);
	    						if (value2 != null) {
	    							String colName = Hasher.hash(col1) + ":" + Hasher.hash(col2);
	    							kvsC.put(output, row1.key(), colName, value1 + "," + value2);
	    						}
	    					}
	    				}
	    			}
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });
	    
	    post("/rdd/fold", (req, res) -> {
	    	// decode query params
	    	String tableName = req.queryParams("tableName");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	String zeroElement = req.queryParams("zeroElement");
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(tableName, startKey, endKey);
	    	String accumulator = zeroElement;
	    	
	    	
	    	TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	while(iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    		if (value != null) {
	    			accumulator = lambda.op(accumulator, value);
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	res.type("/text/plain");
	    	return accumulator;
	    	
	    });
	    
	    post("/rdd/filter", (req, res) -> {
	    	// decode query params
	    	String inputTable = req.queryParams("inputTable");
	    	String outputTable = req.queryParams("outputTable");
	    	String kvs = req.queryParams("kvs");
	    	String startKey = req.queryParams("startKeyRange");
	    	String endKey = req.queryParams("endKeyRange");
	    	
	    	// deserialize lambda, provide the name of the job's JAR file
	    	StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
	    	
	    	// get rows
	    	KVSClient kvsC = new KVSClient(kvs);
	    	Iterator<Row> iter = kvsC.scan(inputTable, startKey, endKey);
	    	
	    	while (iter.hasNext()) {
	    		Row row = iter.next();
	    		String value = row.get("value");
	    		if (value != null) {
	    			if (lambda.op(value)) {
	    				kvsC.put(outputTable, row.key(), "value", value);
	    			}
	    		}
	    	}
	    	
	    	res.status(200, "OK");
	    	return "OK";
	    });

		post("/rdd/cogroupJoin", (req, res) -> {

			String tableOne = req.queryParams("tableOne");
			String tableTwo = req.queryParams("tableTwo");
			String outputTable = req.queryParams("outputTable");
			String kvs = req.queryParams("kvs");
			String startKey = req.queryParams("startKeyRange");
			String endKey = req.queryParams("endKeyRange");
		
			KVSClient kvsC = new KVSClient(kvs);
		
			// Track all keys found in tableOne
			Set<String> keys = new HashSet<>();
		
			// Process tableOne
			Iterator<Row> iter1 = kvsC.scan(tableOne, startKey, endKey);
			while (iter1.hasNext()) {
				Row row1 = iter1.next();
				String key = row1.key();
				keys.add(key);
		
				//List<String> vals1 = new ArrayList<>();
				Set<String> values1 = new HashSet<>();
				for (String col : row1.columns()) {
					String v = row1.get(col);
					if (v != null) values1.add(v);
				}
		
				// Fetch matching row from tableTwo
				Row row2 = kvsC.getRow(tableTwo, key);
		
				//List<String> vals2 = new ArrayList<>();
				Set<String> values2 = new HashSet<>();
				if (row2 != null) {
					for (String col : row2.columns()) {
						String v = row2.get(col);
						if (v != null) values2.add(v);
					}
				}
		
				// Combined output: both lists (even if empty)
				//List<String> combined = new ArrayList<>(vals1);
				Set<String> combined = new HashSet<>(values1);
				combined.addAll(values2);
		
				kvsC.put(outputTable, key, "value", String.join(",", combined));
			}
		
			// Process keys that appear only in tableTwo
			Iterator<Row> iter2 = kvsC.scan(tableTwo, startKey, endKey);
			while (iter2.hasNext()) {
				Row row2 = iter2.next();
				String key = row2.key();
		
				if (keys.contains(key)) continue;
		
				List<String> vals2 = new ArrayList<>();
				for (String col : row2.columns()) {
					String v = row2.get(col);
					if (v != null) vals2.add(v);
				}
		
				kvsC.put(outputTable, key, "value", String.join(",", vals2));
			}
		
			res.status(200, "OK");
			return "OK";
		});		
		
	}
}
