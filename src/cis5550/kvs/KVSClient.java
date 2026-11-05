package cis5550.kvs;

import java.util.*;
import java.net.*;
import java.io.*;
import cis5550.tools.HTTP;
import cis5550.tools.Logger;
import java.util.concurrent.ConcurrentHashMap;

public class KVSClient implements KVS {
  private static final Logger logger = Logger.getLogger(KVSClient.class); 
  String coordinator;
  // fields for batch put:
  private static ConcurrentHashMap<String, List<BatchPutOperation>> batchBuffer = new ConcurrentHashMap<>();
  // Increased from 100 to 2000 for better batching performance in crawler workload
  // Reduces HTTP overhead by sending larger batches less frequently
  private static final int DEFAULT_BATCH_SIZE = 2000;

  // inner class for batch put:
  private static class BatchPutOperation {
    String tableName;
    String row;
    String column;
    byte[] value;

    BatchPutOperation(String table, String r, String c, byte[] v) {
      tableName = table;
      row = r;
      column = c;
      value = v;
    }
  }

  static class WorkerEntry implements Comparable<WorkerEntry> {
    String address;
    String id;

    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  };

  Vector<WorkerEntry> workers;
  boolean haveWorkers;

  public int numWorkers() throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.size();
  }

  public static String getVersion() {
    return "v1.4 Aug 5 2023";
  }

  public String getCoordinator() {
    return coordinator;
  }

  public String getWorkerAddress(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).address;
  }

  public String getWorkerID(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).id;
  }

  class KVSIterator implements Iterator<Row> {
    InputStream in;
    boolean atEnd;
    Row nextRow;
    int currentRangeIndex;
    String endRowExclusive;
    String startRow;
    String tableName;
    Vector<String> ranges;

    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null;
      currentRangeIndex = 0;
      atEnd = false;
      endRowExclusive = endRowExclusiveArg;
      tableName = tableNameArg;
      startRow = startRowArg;
      ranges = new Vector<String>();
      if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
        String url = getURL(tableNameArg, numWorkers()-1, startRowArg, ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0))<0)) ? endRowExclusiveArg : getWorkerID(0));
        ranges.add(url);
      }
      for (int i=0; i<numWorkers(); i++) {
        if ((startRowArg == null) || (i == numWorkers()-1) || (startRowArg.compareTo(getWorkerID(i+1))<0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i))>0);
            boolean useActualEndRow = (endRowExclusiveArg != null) && ((i==(numWorkers()-1)) || (endRowExclusiveArg.compareTo(getWorkerID(i+1))<0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i), useActualEndRow ? endRowExclusiveArg : ((i<numWorkers()-1) ? getWorkerID(i+1) : null));
            ranges.add(url);
          }
        }
      }

      openConnectionAndFill();
    }

    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      String params = "";
      if (startRowArg != null)
        params = "startRow="+startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params+"&"))+"endRowExclusive="+endRowExclusiveArg;
      return "http://"+getWorkerAddress(workerIndexArg)+"/data/"+tableNameArg+(params.equals("") ? "" : "?"+params);
    }

    void openConnectionAndFill() {
      try {
        if (in != null) {
          in.close();
          in = null;
        }

        if (atEnd)
          return;

        while (true) {
          if (currentRangeIndex >= ranges.size()) {
            atEnd = true;
            return;
          } 

          try {
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            in = con.getInputStream();
            Row r = fill();
            if (r != null) {
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
          } catch (URISyntaxException use) {
          }

          currentRangeIndex ++;
        }
      } catch (IOException ioe) {
        if (in != null) {
          try { in.close(); } catch (Exception e) {}
          in = null;
        }
        atEnd = true;
      }
    }

    synchronized Row fill() {
      try {
        Row r = Row.readFrom(in);
        return r;
      } catch (Exception e) {
        return null;
      }
    }

    public synchronized Row next() {
      if (atEnd)
        return null;
      Row r = nextRow;
      nextRow = fill();
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex ++;
        openConnectionAndFill();
      }
      
      return r;
    }

    public synchronized boolean hasNext() {
      return !atEnd;
    }
  }

  synchronized void downloadWorkers() throws IOException {
    String result = new String(HTTP.doRequest("GET", "http://"+coordinator+"/workers", null).body());
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);
    if (numWorkers < 1)
      throw new IOException("No active KVS workers");
    if (pieces.length != (numWorkers+1))
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    workers.clear();
    for (int i=0; i<numWorkers; i++) {
      String[] pcs = pieces[1+i].split(",");
      workers.add(new WorkerEntry(pcs[1], pcs[0]));
    }
    Collections.sort(workers);

    haveWorkers = true;
  }

  int workerIndexForKey(String key) {
    int chosenWorker = workers.size()-1;
    if (key != null) {
      for (int i=0; i<workers.size()-1; i++) {
        if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i+1).id) < 0))
          chosenWorker = i;
      }
    }

    return chosenWorker;
  }

  public KVSClient(String coordinatorArg) {
    coordinator = coordinatorArg;
    workers = new Vector<WorkerEntry>();
    haveWorkers = false;
  }

  public boolean rename(String oldTableName, String newTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    boolean result = true;
    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/rename/"+java.net.URLEncoder.encode(oldTableName, "UTF-8"), newTableName.getBytes()).body();
        String res = new String(response);
        boolean workerSuccess = res.equals("OK");
        result &= workerSuccess;
        
        logger.info("Rename response from worker " + w.address + ": " + res);
        logger.info("Worker address: " + w.address);
        logger.info("Old table name: " + oldTableName);
        logger.info("New table name: " + newTableName);
        
        if (!workerSuccess) {
          System.err.println("ERROR: Worker " + w.address + " returned non-OK response when renaming " + oldTableName + " to " + newTableName + ": " + res);
          logger.error("Worker " + w.address + " failed to rename: " + res);
        }
      } catch (Exception e) {
        result = false;
        System.err.println("ERROR: Exception when renaming on worker " + w.address + ": " + e.getMessage());
        System.err.println("ERROR: Exception type: " + e.getClass().getName());
        e.printStackTrace();
        logger.error("Exception on worker " + w.address + ": " + e.getMessage());
      }
    }

    if (!result) {
      System.err.println("ERROR: Rename operation failed on at least one worker. Old table: " + oldTableName + ", New table: " + newTableName);
      logger.error("Rename failed: " + oldTableName + " -> " + newTableName);
    }

    return result;
  }

  public void delete(String oldTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/delete/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/", null).body();
        String result = new String(response);
        /* TODO: remove this after testing */
        if (!result.equals("OK")) {
          System.err.println("DELETE returned something other than OK: "+result+ "("+w.address+"/delete/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/)");
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }
  }

  public List<String> listTables() throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    Set<String> allTables = new HashSet<>();
    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("GET", "http://"+w.address+"/tables", null).body();
        String result = new String(response);
        // Parse table names from response (each line is a table name)
        String[] lines = result.split("\n");
        for (String line : lines) {
          line = line.trim();
          if (!line.isEmpty()) {
            allTables.add(line);
          }
        }
      } catch (Exception e) {
        // Continue if one worker fails
        logger.warn("Failed to get tables from worker " + w.address + ": " + e.getMessage());
      }
    }
    return new ArrayList<>(allTables);
  }

  public void put(String tableName, String row, String column, byte value[]) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    try {
      String target = "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8");
      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);
      if (!result.equals("OK")) 
      	throw new RuntimeException("PUT returned something other than OK: "+result+ "("+target+")");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    } 
  }

  // modified put method that can optionally batch
  public void put(String tableName, String row, String column, byte value[], boolean batch) throws IOException {
    if (batch) {
      putBatched(tableName, row, column, value);
    } else {
      // call existing method
      put(tableName, row, column, value);
    }
  }

  // batch put operation
  public void putBatched(String tableName, String row, String column, byte value[]) throws IOException {
    // throw errors for null identifiers
    if (tableName == null) {
        throw new IllegalArgumentException("tableName cannot be null");
    }
    if (row == null) {
        throw new IllegalArgumentException("row cannot be null");
    }
    if (column == null) {
        throw new IllegalArgumentException("column cannot be null");
    }
    
    // handle null value by converting to empty byte array
    byte[] actualValue = (value == null) ? new byte[0] : value;
    
    if (!haveWorkers)
        downloadWorkers();
    
    int workerIndex = workerIndexForKey(row);
    String workerAddr = workers.elementAt(workerIndex).address;

    batchBuffer.putIfAbsent(workerAddr, new ArrayList<>());
    batchBuffer.get(workerAddr).add(new BatchPutOperation(tableName, row, column, actualValue));

    if (batchBuffer.get(workerAddr).size() >= DEFAULT_BATCH_SIZE) {
      flush();
    }
  }

  // flush all buffered ops to their respective workers
  public void flush() throws IOException {
    // iterate over buffer, grouped by worker address (key) and list of operations (value)
    for (Map.Entry<String, List<BatchPutOperation>> entry : batchBuffer.entrySet()) {
      // get worker address & list of put ops for this worker
        String workerAddr = entry.getKey();
        List<BatchPutOperation> ops = entry.getValue();
        
        if (ops.isEmpty()) continue;
        
        // Build JSON object w/ an operations array
        StringBuilder json = new StringBuilder("{\"operations\":[");
        boolean first = true;
        for (BatchPutOperation op : ops) {
            // adds a comma b/w ops
            if (!first) json.append(",");
            first = false;
            
            // encodes the bbyte array value to Base64 for JSON
            String base64Value = java.util.Base64.getEncoder().encodeToString(op.value);
            // appens a JSON object for this op: table, row, column, and Base64-encoded value
            json.append(String.format(
                "{\"table\":\"%s\",\"row\":\"%s\",\"column\":\"%s\",\"value\":\"%s\"}",
                escapeJson(op.tableName),
                escapeJson(op.row),
                escapeJson(op.column),
                base64Value
            ));
        }
        json.append("]}");
        
        // Send single HTTP request
        String target = "http://" + workerAddr + "/data/batch";
        byte[] response = HTTP.doRequest("PUT", target, json.toString().getBytes("UTF-8")).body();
        String result = new String(response);
        if (!result.equals("OK")) {
            throw new RuntimeException("Batched PUT returned something other than OK: " + result);
        }
    }
    
    // Clear buffer after successful flush
    batchBuffer.clear();
  }

  // escapes special characters in strings so they can be safely embedded in JSON
  private String escapeJson(String s) {
    return s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
  }

  public void put(String tableName, String row, String column, String value) throws IOException {
    put(tableName, row, column,value.getBytes());
  }

  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    byte[] response = HTTP.doRequest("PUT", "http://"+workers.elementAt(workerIndexForKey(row.key())).address+"/data/"+tableName, row.toByteArray()).body();
    String result = new String(response);
    if (!result.equals("OK")) 
      throw new RuntimeException("PUT returned something other than OK: "+result);
  }

  public Row getRow(String tableName, String row) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response resp = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (resp.statusCode() == 404)
      return null;

    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      throw new RuntimeException("Decoding error while reading Row from getRow() URL");
    }
  }

  public byte[] get(String tableName, String row, String column) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response res = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8"), null);
    return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
  }

  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response r = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    return r.statusCode() == 200;
  }

  public int count(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    int total = 0;
    for (WorkerEntry w : workers) {
      HTTP.Response r = HTTP.doRequest("GET", "http://"+w.address+"/count/"+tableName, null);
      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    } 
    return total;
  }

/*  public void persist(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    for (WorkerEntry w : workers) 
      HTTP.doRequest("PUT", "http://"+w.address+"/persist/"+tableName, null);
  } */

  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    return scan(tableName, null, null);
  }

  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  public static void main(String args[]) throws Exception {
  	if (args.length < 2) {
      System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
  		System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
      System.err.println("Syntax: client <coordinator> scan <tableName>");
      System.err.println("Syntax: client <coordinator> delete <tableName>");
      System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
  		System.exit(1);
  	}

  	KVSClient client = new KVSClient(args[0]);
    if (args[1].equals("put")) {
    	if (args.length != 6) {
	  		System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
	  		System.exit(1);
    	}
      client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
    } else if (args[1].equals("get")) {
      if (args.length != 5) {
        System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
        System.exit(1);
      }
      byte[] val = client.get(args[2], args[3], args[4]);
      if (val == null)
        System.err.println("No value found");
      else 
        System.out.write(val);
    } else if (args[1].equals("scan")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> scan <tableName>");
        System.exit(1);
      }

      Iterator<Row> iter = client.scan(args[2], null, null);
      int count = 0;
      while (iter.hasNext()) {
        System.out.println(iter.next());
        count ++;
      }
      System.err.println(count+" row(s) scanned");
    } else if (args[1].equals("delete")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> delete <tableName>");
        System.exit(1);
      }

      client.delete(args[2]);
      System.err.println("Table '"+args[2]+"' deleted");
    } else if (args[1].equals("rename")) {
      if (args.length != 4) {
        System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
        System.exit(1);
      }
      if (client.rename(args[2], args[3]))
        System.out.println("Success");
      else
        System.out.println("Failure");
    } else {
    	System.err.println("Unknown command: "+args[1]);
    	System.exit(1);
    }
  }
};