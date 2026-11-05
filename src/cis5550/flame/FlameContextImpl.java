package cis5550.flame;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.Serializer;
import java.io.File;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FlameContextImpl implements FlameContext {
	private final String jarName;
	private StringBuilder outputStrings = new StringBuilder();;
	private int sequenceNumber = 0;
	private final long jobID;
	private final SecureRandom random = new SecureRandom();
	private static int counter = 0;
	
	public FlameContextImpl(String jarName) {
		this.jarName = jarName;
		jobID = System.currentTimeMillis();
	}
	
	public KVSClient getKVS() {
		return Coordinator.kvs;
	}

	public void output(String s) {
		  outputStrings.append(s);
	}

	public FlameRDD parallelize(List<String> list) throws Exception {
		System.out.println("Parallelizing list of size " + list.size());
		String tableName = getTableName();
		
		System.out.println("Inserting into table " + tableName);
		KVSClient kvs = getKVS();
		
		for (String value : list) {
			
			byte[] randomBytes = new byte[16];
			random.nextBytes(randomBytes);
			String rowKey = Hasher.hash(
				System.nanoTime() + ":" + java.util.Base64.getEncoder().encodeToString(randomBytes) + 
				":" + counter++
			);

			//kvs.putBatched(tableName, rowKey, "value", value.getBytes());
			kvs.put(tableName, rowKey, "value", value.getBytes());
		}
		//kvs.flush();
		
		return new FlameRDDImpl(tableName, this);
		
	}
	
	public String concatenateOutputStrings() {
		return outputStrings.toString();
	}
	
	public String getTableName() {
		sequenceNumber++;
		String tableName = "" + jobID + ":" + sequenceNumber;
		return tableName;
	}
	
	// This function should scan the table in the key-value store with the specified name, 
	// invoke the provided lambda with each Row of data from the KVS, and then return
	// and RDD with all the strings that the lambda invocations returned. The lambda
	// is allowed to return null for certain Rows; when it does, no data should be
	// added to the RDD for these Rows. This method should run in parallel on all the
	// workers, just like the RDD/PairRDD operations.
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		Serializer s = new Serializer();
		byte[] lambdaBytes = s.objectToByteArray(lambda);
		return new FlameRDDImpl(invokeOperation("/rdd/fromTable", lambdaBytes, tableName, null), this);
	}
	
	public String invokeOperation(String operation, byte[] lambda, String inputTableName, String zeroElement) throws Exception {
		// generate a fresh name for the output table
		sequenceNumber++;
		String outputTableName = "" + jobID + ":" + sequenceNumber;
		
		// use Partitioner to find a good assignment of key ranges to Flame workers
		Partitioner part = new Partitioner();
		
		// PERFORMANCE FIX: Reduced from 16 to 4 partitions per worker
		// With improved URL hashing (path+host), we get better natural distribution
		// Fewer partitions = less HTTP overhead and fewer empty partition scans
		part.setKeyRangesPerWorker(4);
		
		// invoke addKVSWorkers for each KVS worker & give it the worker's address, worker's ID
		// as the start of KVS key range, and next worker's ID as end of that range
		KVSClient kvs = getKVS();
		int numWorkers = kvs.numWorkers();
		for (int i = 0; i < numWorkers; i++) {
			String address = kvs.getWorkerAddress(i);
			String starKey = kvs.getWorkerID(i);
			String endKey = kvs.getWorkerID((i + 1) % numWorkers);
			
			// call addKVSWorker 2x for last worker
			if (i == numWorkers - 1) {
				part.addKVSWorker(address, starKey, null);
				part.addKVSWorker(address, null, endKey);
			} else {
				part.addKVSWorker(address, starKey, endKey);
			}
		}
		// call addFlameWorker for each Flame worker
		List<String> flameWorkers = Coordinator.getWorkers();
		for (String flameWorker : flameWorkers) {
			part.addFlameWorker(flameWorker);
		}
		// call partitioner's assignPartitions() 
		Vector<Partition> parts = part.assignPartitions();
		
		// send HTTP request to each worker to tell it what op to perform, use
		// HTTP.doRequest & send all requests in parallel (from multiple threads)
		Thread[] threads = new Thread[parts.size()];
		List<Exception> errors = new ArrayList<>();
		
		for (int i = 0; i < threads.length; i++) {
			Partition p = parts.get(i);
			
			Thread thread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append(operation);
					reqURL.append("?inputTable=" + URLEncoder.encode(inputTableName, "UTF-8"));
					reqURL.append("&outputTable=" + URLEncoder.encode(outputTableName, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
					
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
					if (zeroElement != null) {
						reqURL.append("&zeroElement=" + URLEncoder.encode(zeroElement, "UTF-8"));
					}
				
					Response res = HTTP.doRequest("POST", reqURL.toString(), lambda);
					
					if (res.statusCode() != 200) {
						String errorBody = new String(res.body());
						String fullError = "status code " + res.statusCode() + " from Worker" + 
					p.kvsWorker + ": " + errorBody;
						// Log the full error details
						System.err.println("FLAME OPERATION ERROR: " + fullError);
						System.err.println("Request URL: " + reqURL.toString());
						System.err.println("Response body length: " + errorBody.length());
						System.err.println("Response body: " + errorBody);
						errors.add(new RuntimeException(fullError));
					}
					
				} catch (Exception e) {
					errors.add(new RuntimeException(e));
				}
				
				
			});
			threads[i] = thread;
			thread.start();
			
		}
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : threads) {
			thread.join();
		}
		
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
		
		return outputTableName;
	}
	
	public String intersection(String originalTable, String otherTable) throws Exception {
		sequenceNumber++;
		String noDuplicatesTable = "" + jobID + ":" + sequenceNumber;
		sequenceNumber++;
		String outputTable = "" + jobID + ":" + sequenceNumber;
		
		// use Partitioner to find a good assignment of key ranges to Flame workers
		Partitioner part = new Partitioner();
		
		// invoke addKVSWorkers for each KVS worker & give it the worker's address, worker's ID
		// as the start of KVS key range, and next worker's ID as end of that range
		KVSClient kvs = getKVS();
		int numWorkers = kvs.numWorkers();
		for (int i = 0; i < numWorkers; i++) {
			String address = kvs.getWorkerAddress(i);
			String starKey = kvs.getWorkerID(i);
			String endKey = kvs.getWorkerID((i + 1) % numWorkers);
			
			// call addKVSWorker 2x for last worker
			if (i == numWorkers - 1) {
				part.addKVSWorker(address, starKey, null);
				part.addKVSWorker(address, null, endKey);
			} else {
				part.addKVSWorker(address, starKey, endKey);
			}
		}
		// call addFlameWorker for each Flame worker
		List<String> flameWorkers = Coordinator.getWorkers();
		for (String flameWorker : flameWorkers) {
			part.addFlameWorker(flameWorker);
		}
		// call partitioner's assignPartitions() 
		Vector<Partition> parts = part.assignPartitions();
		
		// send HTTP request to each worker for deduplication
		// parallelism requirement continues to apply, so use multiple threads to remove duplicates
		Thread[] deduplicationThreads = new Thread[parts.size()];
		List<Exception> deduplicationErrors = new ArrayList<>();
		
		for (int i = 0; i < deduplicationThreads.length; i++) {
			Partition p = parts.get(i);
			
			Thread deduplicationThread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append("/rdd/deduplicate");
					reqURL.append("?originalTable=" + URLEncoder.encode(originalTable, "UTF-8"));
					reqURL.append("&uniqueTable=" + URLEncoder.encode(noDuplicatesTable, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
					
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
					
					Response res = HTTP.doRequest("POST", reqURL.toString(), null);
					
					if (res.statusCode() != 200) {
						deduplicationErrors.add(new RuntimeException("status code " + res.statusCode() + " from Worker" +
						p.kvsWorker));
					}
					
				} catch (Exception e) {
					deduplicationErrors.add(new RuntimeException(e));
				}	
			});
			deduplicationThreads[i] = deduplicationThread;
			deduplicationThread.start();
			
		}
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : deduplicationThreads) {
			thread.join();
		}
		
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!deduplicationErrors.isEmpty()) {
			throw deduplicationErrors.get(0);
		}
		
		// send HTTP request to each worker to actually get overlapping values from comparison
		// parallism requirement still applies, use multiple threads 
		Thread[] intersectionThreads = new Thread[parts.size()];
		List<Exception> intersectionErrors = new ArrayList<>();
		
		for (int i = 0; i < intersectionThreads.length; i++) {
			Partition p = parts.get(i);
			
			Thread intersectionThread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append("/rdd/intersection");
					reqURL.append("?uniqueTable=" + URLEncoder.encode(noDuplicatesTable, "UTF-8"));
					reqURL.append("&otherTable=" + URLEncoder.encode(otherTable, "UTF-8"));
					reqURL.append("&outputTable=" + URLEncoder.encode(outputTable, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
					
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
					
					Response res = HTTP.doRequest("POST", reqURL.toString(), null);
					
					if (res.statusCode() != 200) {
						deduplicationErrors.add(new RuntimeException("status code " + res.statusCode() + " from Worker" +
						p.kvsWorker));
					}
					
				} catch (Exception e) {
					intersectionErrors.add(new RuntimeException(e));
				}
				
				
			});
			intersectionThreads[i] = intersectionThread;
			intersectionThread.start();
			
		}
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : intersectionThreads) {
			thread.join();
		}
		
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!intersectionErrors.isEmpty()) {
			throw intersectionErrors.get(0);
		}
		
		return outputTable;	
	}
	
	public String join(String tableOne, String tableTwo) throws Exception {
		// generate a fresh name for the output table
		sequenceNumber++;
		String outputTableName = "" + jobID + ":" + sequenceNumber;
		
		// use Partitioner to find a good assignment of key ranges to Flame workers
		Partitioner part = new Partitioner();
		
		// invoke addKVSWorkers for each KVS worker & give it the worker's address, worker's ID
		// as the start of KVS key range, and next worker's ID as end of that range
		KVSClient kvs = getKVS();
		int numWorkers = kvs.numWorkers();
		for (int i = 0; i < numWorkers; i++) {
			String address = kvs.getWorkerAddress(i);
			String starKey = kvs.getWorkerID(i);
			String endKey = kvs.getWorkerID((i + 1) % numWorkers);
			
			// call addKVSWorker 2x for last worker
			if (i == numWorkers - 1) {
				part.addKVSWorker(address, starKey, null);
				part.addKVSWorker(address, null, endKey);
			} else {
				part.addKVSWorker(address, starKey, endKey);
			}
		}
		// call addFlameWorker for each Flame worker
		List<String> flameWorkers = Coordinator.getWorkers();
		for (String flameWorker : flameWorkers) {
			part.addFlameWorker(flameWorker);
		}
		// call partitioner's assignPartitions() 
		Vector<Partition> parts = part.assignPartitions();
		
		// send HTTP request to each worker to tell it what op to perform, use
		// HTTP.doRequest & send all requests in parallel (from multiple threads)
		Thread[] threads = new Thread[parts.size()];
		List<Exception> errors = new ArrayList<>();
		
		for (int i = 0; i < threads.length; i++) {
			Partition p = parts.get(i);
			
			Thread thread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append("/rdd/join");
					reqURL.append("?tableOne=" + URLEncoder.encode(tableOne, "UTF-8"));
					reqURL.append("&tableTwo=" + URLEncoder.encode(tableTwo, "UTF-8"));
					reqURL.append("&outputTable=" + URLEncoder.encode(outputTableName, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
					
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
			
					Response res = HTTP.doRequest("POST", reqURL.toString(), null);
					
					if (res.statusCode() != 200) {
						errors.add(new RuntimeException("status code " + res.statusCode() + " from Worker" + 
					p.kvsWorker));
					}
					
				} catch (Exception e) {
					errors.add(new RuntimeException(e));
				}
				
				
			});
			threads[i] = thread;
			thread.start();
			
		}
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : threads) {
			thread.join();
		}
		
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
		
		return outputTableName;
	}
	
	public String fold(String tableName, String zeroElement, byte[] lambda) throws Exception{
		List<String> stringParts = new ArrayList<>();
		
		// use Partitioner to find a good assignment of key ranges to Flame workers
		Partitioner part = new Partitioner();
		
		// invoke addKVSWorkers for each KVS worker & give it the worker's address, worker's ID
		// as the start of KVS key range, and next worker's ID as end of that range
		KVSClient kvs = getKVS();
		int numWorkers = kvs.numWorkers();
		for (int i = 0; i < numWorkers; i++) {
			String address = kvs.getWorkerAddress(i);
			String starKey = kvs.getWorkerID(i);
			String endKey = kvs.getWorkerID((i + 1) % numWorkers);
			
			// call addKVSWorker 2x for last worker
			if (i == numWorkers - 1) {
				part.addKVSWorker(address, starKey, null);
				part.addKVSWorker(address, null, endKey);
			} else {
				part.addKVSWorker(address, starKey, endKey);
			}
		}
		// call addFlameWorker for each Flame worker
		List<String> flameWorkers = Coordinator.getWorkers();
		for (String flameWorker : flameWorkers) {
			part.addFlameWorker(flameWorker);
		}
		// call partitioner's assignPartitions() 
		Vector<Partition> parts = part.assignPartitions();
		
		// send HTTP request to each worker to tell it what op to perform, use
		// HTTP.doRequest & send all requests in parallel (from multiple threads)
		Thread[] threads = new Thread[parts.size()];
		List<Exception> errors = new ArrayList<>();
		
		for (int i = 0; i < threads.length; i++) {
			Partition p = parts.get(i);
			
			Thread thread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append("/rdd/fold");
					reqURL.append("?tableName=" + URLEncoder.encode(tableName, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
					reqURL.append("&zeroElement=" + URLEncoder.encode(zeroElement, "UTF-8"));
					
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
				
					Response res = HTTP.doRequest("POST", reqURL.toString(), lambda);
					
					if (res.statusCode() != 200) {
						errors.add(new RuntimeException("status code " + res.statusCode() + " from Worker" + 
					p.kvsWorker));
					} else {
						stringParts.add(new String(res.body()));
					}
					
				} catch (Exception e) {
					errors.add(new RuntimeException(e));
				}
				
				
			});
			threads[i] = thread;
			thread.start();
			
		}
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : threads) {
			thread.join();
		}
		
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
		
		// final round of aggregation
		TwoStringsToString l = (TwoStringsToString) Serializer.byteArrayToObject(lambda, new File(this.jarName));;
		
		String accumulator = zeroElement;
		for (String s : stringParts) {
			accumulator = l.op(accumulator, s);
		}
		
		return accumulator;
	}
	public String cogroupJoin(String tableOne, String tableTwo) throws Exception {
		sequenceNumber++;
		String outputTableName = "" + jobID + ":" + sequenceNumber;
	
		// use Partitioner to find a good assignment of key ranges to Flame workers
		Partitioner part = new Partitioner();
	
		// invoke addKVSWorkers for each KVS worker & give it the worker's address, worker's ID
		// as the start of KVS key range, and next worker's ID as end of that range
		KVSClient kvs = getKVS();
		int numWorkers = kvs.numWorkers();
		for (int i = 0; i < numWorkers; i++) {
			String address = kvs.getWorkerAddress(i);
			String starKey = kvs.getWorkerID(i);
			String endKey = kvs.getWorkerID((i + 1) % numWorkers);
	
			// call addKVSWorker 2x for last worker
			if (i == numWorkers - 1) {
				part.addKVSWorker(address, starKey, null);
				part.addKVSWorker(address, null, endKey);
			} else {
				part.addKVSWorker(address, starKey, endKey);
			}
		}
		// call addFlameWorker for each Flame worker
		List<String> flameWorkers = Coordinator.getWorkers();
		for (String flameWorker : flameWorkers) {
			part.addFlameWorker(flameWorker);
		}
		// call partitioner's assignPartitions()
		Vector<Partition> parts = part.assignPartitions();
	
		// send HTTP request to each worker to tell it what op to perform, use
		// HTTP.doRequest & send all requests in parallel (from multiple threads)
		Thread[] threads = new Thread[parts.size()];
		List<Exception> errors = new ArrayList<>();
	
		for (int i = 0; i < threads.length; i++) {
			Partition p = parts.get(i);
	
			Thread thread = new Thread(() -> {
				try {
					StringBuilder reqURL = new StringBuilder("http://");
					reqURL.append(p.assignedFlameWorker);
					reqURL.append("/rdd/cogroupJoin");
					reqURL.append("?tableOne=" + URLEncoder.encode(tableOne, "UTF-8"));
					reqURL.append("&tableTwo=" + URLEncoder.encode(tableTwo, "UTF-8"));
					reqURL.append("&outputTable=" + URLEncoder.encode(outputTableName, "UTF-8"));
					reqURL.append("&kvs=" + kvs.getCoordinator());
	
					// append startKey and endKey if not at wrap around key ranges (null)
					if (p.fromKey != null) {
						reqURL.append("&startKeyRange=" + URLEncoder.encode(p.fromKey, "UTF-8"));
					}
					if (p.toKeyExclusive != null) {
						reqURL.append("&endKeyRange=" + URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
					}
	
					Response res = HTTP.doRequest("POST", reqURL.toString(), null);
	
					if (res.statusCode() != 200) {
						synchronized (errors) {
							errors.add(new RuntimeException("status code " + res.statusCode() + " from Worker" +
								p.kvsWorker));
						}
					}
	
				} catch (Exception e) {
					synchronized (errors) {
						errors.add(new RuntimeException(e));
					}
				}
			});
	
			threads[i] = thread;
			thread.start();
		}
	
		// wait for responses to arrive (Thread.join()) and check for failures, then report back to caller
		for (Thread thread : threads) {
			thread.join();
		}
	
		// check whether any requests failed or returned status codes other than 200, your function should
		// report this to the caller
		if (!errors.isEmpty()) {
			throw errors.get(0);
		}
	
		return outputTableName;
	}
	
}
	
	