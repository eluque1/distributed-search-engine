package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.HashSet;
import java.util.Set;

import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import cis5550.tools.Logger;

public class FlameRDDImpl implements FlameRDD {
	private String tableName;
	private FlameContextImpl fctx;
	private static final Logger logger = Logger.getLogger(FlameRDDImpl.class);
	  public FlameRDDImpl(String tableName, FlameContextImpl fctx) {
		this.tableName = tableName;
		this.fctx = fctx;
	}

	  public List<String> collect() throws Exception {
		  KVSClient kvs = Coordinator.kvs;
		  List<String> values = new ArrayList<String>();
		  
		  Iterator<Row> iter = kvs.scan(tableName);
		  while (iter.hasNext()) {
			  Row row = iter.next();
			  String value = row.get("value");
			  values.add(value); 
		  }
		  
		  return values;
	  }
	  
	  // count() should return the number of elements in this RDD. 
	  // Duplicate elements should be included in the count.
	  public int count() throws Exception {
		  int count = 0;
		  KVSClient kvs = Coordinator.kvs;
		  Iterator<Row> iter = kvs.scan(tableName);
		  
		  while (iter.hasNext()) {
			  Row row = iter.next();
			  String value = row.get("value");
			  count++;
		  }
		  
		  return count;
	  }

	  // saveAsTable() should cause a table with the specified name to appear 
	  // in the KVS that contains the data from this RDD. The table should 
	  // have a row for each element of the RDD, and the element should be
	  // in a column called 'value'; the key can be anything. 
	  public void saveAsTable(String tableNameArg) throws Exception {
		logger.info("In saveAsTable - Table name: " + this.tableName);
		logger.info("In saveAsTable - Table name arg: " + tableNameArg);
		Coordinator.kvs.rename(this.tableName, tableNameArg);
		this.tableName = tableNameArg;
	  }
	  
	  // take() should return up 'num' elements from the RDD, or all
	  // elements, if the RDD contains fewer than 'num' elements.
	  // If the RDD contains more than 'num' elements, any subset
	  // of size 'num' may be returned.
	  public Vector<String> take(int num) throws Exception {
		  KVSClient kvs = Coordinator.kvs;
		  Vector<String> output = new Vector<String>();
		  Iterator<Row> iter = kvs.scan(tableName);
		  
		  int count = num;
		  while (iter.hasNext() && count > 0) {
			  Row row = iter.next();
			  String value = row.get("value");
			  output.add(value);
			  count--;
		  }
		  
		  return output;
	  }

	  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/flatMap", serializedLambda, tableName, null);
		  return new FlameRDDImpl(newTableName, fctx);
	  }

	  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/mapToPair", serializedLambda, tableName, null);
		  return new FlamePairRDDImpl(newTableName, fctx);
	  }
	  
	  // flatMapToPair() is analogous to flatMap(), except that the lambda
	  // returns pairs instead of strings, and that the output is a PairRDD
	  // instead of a normal RDD.

	  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, null);
		  return new FlamePairRDDImpl(newTableName, fctx);
	  }  

	  // distinct() should return a new RDD that contains the same
	  // elements, except that, if the current RDD contains multiple
	  // copies of some elements, the new RDD should contain only 
	  // one copy of those elements.

	  public FlameRDD distinct() throws Exception {
		  KVSClient kvs = Coordinator.kvs;
		  String newTableName = fctx.getTableName();
		  
		  Iterator<Row> iter = kvs.scan(this.tableName);
		  Set<String> seen = new HashSet<>();
		  while (iter.hasNext()) {
			  Row row = iter.next();
			  String value = row.get("value");
			  if (value != null && !seen.contains(value)) {
				seen.add(value);
				kvs.putBatched(newTableName, value, "value", value.getBytes());
			  }
		  }
		  kvs.flush();

		  return new FlameRDDImpl(newTableName, fctx);
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

	  // fold() should call the provided lambda for each element of the 
	  // RDD, with that element as the second argument. In the first
	  // invocation, the first argument should be 'zeroElement'; in
	  // each subsequent invocation, the first argument should be the
	  // result of the previous invocation. The function returns
	  // the result of the last invocation, or 'zeroElement' if the 
	  // RDD does not contain any elements.

	  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String output = fctx.fold(this.tableName, zeroElement, serializedLambda);
		  return output;
	  }
	  

	  // EXTRA CREDIT:
	  public FlameRDD intersection(FlameRDD r) throws Exception {
		  String otherTable = ((FlameRDDImpl) r).getTableName();
		  String newTableName = fctx.intersection(this.tableName, otherTable);
		  return new FlameRDDImpl(newTableName, fctx);
	  }

	  // EXTRA CREDIT:
	  public FlameRDD sample(double f) throws Exception {
		  return null;
	  }
	  
	  // EXTRA CREDIT:
	  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		  FlamePairRDD keyed = this.mapToPair(s -> new FlamePair(lambda.op(s), s));
		  FlamePairRDD combined = keyed.foldByKey(
				  "", (acc, value) -> {
		  if (acc.isEmpty()) {
			  return value;
		  } else {
			  return acc + "," + value;
			  }
		  });
		  return combined;
	  }
	  
	  public String getTableName() {
		  return this.tableName;
	  }
	  
	  // EXTRA CREDIT
	  // filter() should accept a boolean predicate and should return 
	  // another RDD that contains only those elements from the original RDD 
	  // on which the predicate evaluates to true. This method is extra credit
	  // on HW7 and should return 'null' if you did not do this EC.
	  public FlameRDD filter(StringToBoolean lambda) throws Exception {
		  Serializer s = new Serializer();
		  byte[] serializedLambda = s.objectToByteArray(lambda);
		  String newTableName = fctx.invokeOperation("/rdd/filter", serializedLambda, tableName, null);
		  return new FlameRDDImpl(newTableName, fctx);
	  }

	  // EXTRA CREDIT
	  // mapPartitions() should take a lambda that is given an Iterator<String>
	  // and returns another Iterator<String>. The lambda should be invoked once 
	  // on each worker, with an iterator that contains the RDD elements 
	  // that worker is working on (see KVSClient.scan()); the elements 
	  // in the iterator that the lamblda returns should be stored in 
	  // another RDD, which mapPartitions() should return. This method is 
	  // extra credit on HW7 and should return 'null' if you did not do this EC.
	  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		  return null;
	  }

}
