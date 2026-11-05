package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	private String tableName;
	private FlameContextImpl fctx;

	public FlamePairRDDImpl(String tableName, FlameContextImpl fctx) {
		this.tableName = tableName;
		this.fctx = fctx;
	}
	  
	public List<FlamePair> collect() throws Exception {
		KVSClient kvs = Coordinator.kvs;
		List<FlamePair> values = new ArrayList<FlamePair>();
		
		Iterator<Row> iter = kvs.scan(tableName);
		while(iter.hasNext()) {
			Row row = iter.next();
			Set<String> cols = row.columns();
			for (String col : cols) {
				String value = row.get(col);
				values.add(new FlamePair(row.key(), value));
			}
		}
		return values;
	}

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		Serializer s = new Serializer();
		byte[] serializedLambda = s.objectToByteArray(lambda);
		String newTableName = fctx.invokeOperation("/rdd/foldByKey", serializedLambda, tableName, zeroElement);
		return new FlamePairRDDImpl(newTableName, fctx);
	}
	
	// saveAsTable() should cause a table with the specified name to appear 
	  // in the KVS that contains the data from this PairRDD. The table should 
	  // have a row for each unique key in the PairRDD, and the different values
	  // that are associated with this key should be in different columns. The
	  // names of the columns can be anything.

	  public void saveAsTable(String tableNameArg) throws Exception {
		  try {
			  System.out.println("saveAsTable: Attempting to rename table from " + tableName + " to " + tableNameArg);
			  
			  // Best-effort cleanup of target to avoid rename collisions
			  try {
				  Coordinator.kvs.delete(tableNameArg);
				  System.out.println("saveAsTable: Deleted existing target table (if any): " + tableNameArg);
			  } catch (Exception cleanupEx) {
				  System.err.println("saveAsTable: Warning - could not delete existing target table " + tableNameArg + " before rename: " + cleanupEx.getMessage());
			  }
			  
			  // Verify source table exists before attempting rename
			  try {
				  java.util.List<String> allTables = Coordinator.kvs.listTables();
				  if (!allTables.contains(tableName)) {
					  System.err.println("saveAsTable: WARNING - Source table " + tableName + " not found in list of tables. Available tables: " + allTables);
					  // Continue anyway - table might be distributed and not visible in listTables
				  } else {
					  System.out.println("saveAsTable: Verified source table " + tableName + " exists");
				  }
			  } catch (Exception e) {
				  System.err.println("saveAsTable: Could not verify table existence: " + e.getMessage());
				  // Continue anyway
			  }
			  
			  boolean renameSuccess = false;
			  int attempts = 0;
			  while (!renameSuccess && attempts < 3) {
				  if (attempts > 0) {
					  System.err.println("saveAsTable: Retry " + attempts + " deleting target " + tableNameArg + " and retrying rename");
					  try {
						  Coordinator.kvs.delete(tableNameArg);
					  } catch (Exception retryDelEx) {
						  System.err.println("saveAsTable: Warning - could not delete target on retry: " + retryDelEx.getMessage());
					  }
				  }
				  renameSuccess = Coordinator.kvs.rename(tableName, tableNameArg);
				  attempts++;
				  if (!renameSuccess) {
					  try { Thread.sleep(100); } catch (InterruptedException ie) { /* ignore */ }
				  }
			  }

			  if (!renameSuccess) {
				  System.err.println("saveAsTable: ERROR - rename returned false when renaming " + tableName + " to " + tableNameArg);
				  // List tables after failed rename to help debug
				  try {
					  java.util.List<String> tablesAfterRename = Coordinator.kvs.listTables();
					  System.err.println("saveAsTable: Tables after failed rename: " + tablesAfterRename);
					  System.err.println("saveAsTable: Source table " + tableName + " exists: " + tablesAfterRename.contains(tableName));
					  System.err.println("saveAsTable: Target table " + tableNameArg + " exists: " + tablesAfterRename.contains(tableNameArg));
				  } catch (Exception e2) {
					  System.err.println("saveAsTable: Could not list tables after failed rename: " + e2.getMessage());
				  }
				  throw new Exception("Failed to rename table from " + tableName + " to " + tableNameArg + " (rename returned false)");
			  }
			  System.out.println("saveAsTable: SUCCESS - renamed table from " + tableName + " to " + tableNameArg);
			  this.tableName = tableNameArg;
		  } catch (Exception e) {
			  System.err.println("saveAsTable: EXCEPTION when renaming table from " + tableName + " to " + tableNameArg);
			  System.err.println("saveAsTable: Exception message: " + e.getMessage());
			  System.err.println("saveAsTable: Exception type: " + e.getClass().getName());
			  System.err.println("saveAsTable: Stack trace:");
			  e.printStackTrace();
			  throw e; // Re-throw the exception
		  }
	  }
	  
	  // flatMap() should invoke the provided lambda once for each pair in the PairRDD, 
	  // and it should return a new RDD that contains all the strings from the Iterables 
	  // the lambda invocations have returned. It is okay for the same string to appear 
	  // more than once in the output; in this case, the RDD should contain multiple 
	  // copies of that string. The lambda is allowed to return null or an empty Iterable.

	  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/flatMap", serializedLambda, tableName, null);
		  return new FlameRDDImpl(newTableName, fctx);
	  }

	  // flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs 
	  // instead of strings, and that the output is a PairRDD instead of a normal RDD.

	  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, null);
		  return new FlamePairRDDImpl(newTableName, fctx);
	  }
	  
	  // destroy() should delete the underlying table in the key-value store.
	  // Any future invocations of any method on this RDD should throw an
	  // exception.

	  public void destroy() throws Exception {
		  String tableToDelete = tableName;
		  System.out.println("Destroy: Attempting to delete table: " + tableToDelete);
		  
		  Coordinator.kvs.delete(tableToDelete);
		  
		  // Check if deletion was successful by trying to count the table
		  try {
			  int count = Coordinator.kvs.count(tableToDelete);
			  if (count == 0) {
				  System.out.println("Destroy: SUCCESS - Table " + tableToDelete + " deleted (count is 0)");
			  } else {
				  System.out.println("Destroy: FAILED - Table " + tableToDelete + " still exists with " + count + " rows");
			  }
		  } catch (Exception e) {
			  // Table might not exist anymore, which is good
			  System.out.println("Destroy: SUCCESS - Table " + tableToDelete + " deleted (count throws exception: " + e.getMessage() + ")");
		  }
		  
		  this.tableName = null;
	  }

	  // join() joins the current PairRDD A with another PairRDD B. Suppose A contains
	  // a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
	  // a pair (k,v_A+","+v_B).

	  public FlamePairRDD join(FlamePairRDD other) throws Exception {
		  Serializer s = new Serializer();
		  String otherTable = ((FlamePairRDDImpl) other).getTableName();
		  String newTableName = fctx.join(tableName, otherTable);
		  return new FlamePairRDDImpl(newTableName, fctx);
	  }

	  private String getTableName() {
		  return this.tableName;
	  }

	  
	  
	  // EXTRA CREDIT
	  // This method should return a new PairRDD that contains, for each key k that exists 
	  // in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are 
	  // comma-separated lists of the values from the original RDD and from R, respectively. 
	  // For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and 
	  // R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain 
	  // a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is 
	  // extra credit in HW7; if you do not implement it, please return 'null'.
	  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		  return null;
	  }

	  // This method should return a new PairRDD that contains, for each key k that exists 
	  // in either the original RDD or in R, a pair (k,"X,Y"), where X and Y are 
	  // comma-separated lists of the values from the original RDD and from R, respectively. 
	  // For instance, if the original RDD contains (fruit,apple) (vegetable,carrot) and (fruit,banana) and 
	  // R contains (fruit,cherry), (vegetable,broccoli), (fruit,date) and (fruit,fig), the result should contain 
	  // a pair with key fruit and value "apple,banana,cherry,date,fig" and a pair with key vegetable and value "carrot,broccoli". This method is 
	  // extra credit in HW7; if you do not implement it, please return 'null'.

	public FlamePairRDD cogroupJoin(FlamePairRDD other) throws Exception {
		String otherTable = ((FlamePairRDDImpl) other).getTableName();
		String newTableName = fctx.cogroupJoin(tableName, otherTable);
		return new FlamePairRDDImpl(newTableName, fctx);
	}



}
