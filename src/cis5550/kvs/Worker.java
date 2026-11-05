package cis5550.kvs;
import cis5550.webserver.Server;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import cis5550.tools.KeyEncoder;
import java.io.ByteArrayInputStream;
import java.net.URLEncoder;
import java.net.URLDecoder;
import cis5550.tools.Logger;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.text.DefaultStyledDocument;
import java.nio.file.StandardOpenOption;

public class Worker extends cis5550.generic.Worker{
    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables=new ConcurrentHashMap<>();

    // cache for persistent tables
    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> ptCache =  new ConcurrentHashMap<>();
    private static final int CACHE_MAX_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length!=3){
            System.err.println("wrong number of arguments in command line");
            System.exit(1);
        }
        int workerport=0;
        try {
            workerport=Integer.parseInt(args[0]); //remember to turn stirng to int
        } 
        catch ( NumberFormatException e) {
            System.err.println("the first argument(port) has to be a valid integer");
            System.exit(1);
        }   
        final Logger logger = Logger.getLogger(Worker.class);
        String storagedir=args[1];
        String coordinatoraddy=args[2];
        Server.port(workerport);
        String workerid=null;
        Path idfilepath=Paths.get(storagedir, "id");
        File storagedirfile= new File(storagedir);
        try {
            if (!storagedirfile.exists()){
                if(!storagedirfile.mkdirs()){
                    System.err.println("failed to dcreate storage dir");
                    System.exit(1);
                }
            }
            if(Files.exists(idfilepath)){
                workerid=Files.readAllLines(idfilepath, StandardCharsets.UTF_8).get(0).trim();

                System.err.println("worker id read from file:"+workerid);
            }
            else{
                Random random = new  Random();
                String newid = ""; 
                for (int i=0; i<5; i++) {
                    char randomChar = (char)(random.nextInt(26) + 'a');
                    newid = newid+randomChar; 
                }
                workerid = newid;
                Files.write(idfilepath, workerid.getBytes(StandardCharsets.UTF_8));
                System.err.println("New Worker ID generated and written to file: " + workerid);
            } 
            if(workerid==null||workerid.isEmpty()){
                throw new IOException("worker id is empty");
            }
        } 
        catch (IOException e) {
            System.err.println("error accessing or writing worker id file");
            System.exit(1);
        }
        Server.put("/data/:table/:row/:column", (req, res) -> {
            String table=req.params("table");
            String row=req.params("row");
            String column=req.params("column");
            byte[] data =req.bodyAsBytes();
            if(table.startsWith("pt-")){
                try {
                    Path tablepath=Paths.get(storagedir, table);
                    if(!Files.exists(tablepath)){
                        Files.createDirectories(tablepath); //basically checking if the table exists in the perssistent contxt, by checking if there is a file that exists with that path and name, if not make it happen
                    }
                    String filename=KeyEncoder.encode(row); //usin the helper to get a safe fil nemae
                    Path filepath=tablepath.resolve(filename);
                    System.out.println("DEBUGGING: this is the filepath we are trying to write to:" +filepath.toAbsolutePath());
                    //now for the updates gotta readmodify and hten write
                    Row row1;
                    if (Files.exists(filepath)){
                        byte[] existingdata=Files.readAllBytes(filepath); //if the file exists read all the bytes form the file
                        ByteArrayInputStream inputStream=new ByteArrayInputStream(existingdata); //there was an error when i treid converting back from existingdata itself so wrapping it in an input stream thing
                        row1=Row.readFrom(inputStream); //turn bytes back into row?
                    }
                    else{
                        row1=new Row(row);
                    }
                    row1.put(column, data);
                    Files.write(filepath, row1.toByteArray());

                    // Add to cache
                    cachePut(table, row, row1);

                    res.body("OK");
                    return "OK";
                } catch (Exception e) {
                    e.printStackTrace();
                    String errorMsg = "Error writing to table " + table + ", row " + row + ", column " + column + ": " + e.getClass().getSimpleName() + ": " + e.getMessage();
                    // Log to both logger and stderr for visibility
                    logger.error(errorMsg, e);
                    System.err.println("KVS WORKER ERROR: " + errorMsg);
                    e.printStackTrace(System.err);
                    // Try to include error in status message (truncated if too long)
                    String statusMsg = errorMsg.length() > 100 ? errorMsg.substring(0, 100) + "..." : errorMsg;
                    res.status(500, statusMsg);
                    res.body(errorMsg);
                    return errorMsg;
                }
            } 
            else{ //this is if it is an in memory table
                tables.putIfAbsent(table, new ConcurrentHashMap<>());
                ConcurrentHashMap<String, Row> tabledata=tables.get(table);
                tabledata.putIfAbsent(row, new Row(row));
                Row rowdata=tabledata.get(row); //im beginnging doing this pls work gradescope
                rowdata.put(column, data);
                res.body("OK");
                return "OK";
            }   
        });
        Server.put("/data/batch", (req, res) -> {
            try {
                String jsonStr = new String(req.bodyAsBytes(), "UTF-8");

                int opsStart = jsonStr.indexOf("\"operations\":[")+ 14;
                int opsEnd = jsonStr.lastIndexOf("]");
                if (opsStart < 14 || opsEnd < opsStart) 
                    throw new Exception("Invalid JSON format");
                String opsStr = jsonStr.substring(opsStart, opsEnd + 1);

                // parse each op
                String[] operations;
                if (opsStr.startsWith("{}") && opsStr.endsWith("}")) {
                    if (opsStr.contains("},{")) {
                        operations = opsStr.split("\\},\\{");
                    } else {
                        // single operation, wrap in array
                        operations = new String[]{opsStr};
                    }
                } else {
                    operations = opsStr.split("\\},\\{");
                }
                for (String op : operations) {
                    op = op.replace("{", "").replace("}", "");

                    // extract fields
                    String table = extractJsonField(op, "table");
                    String row = extractJsonField(op, "row");
                    String column = extractJsonField(op, "column");
                    String base64Value = extractJsonField(op, "value");
                    
                    // Validate all fields, if null, skip op
                    if (table == null || row == null || column == null || base64Value == null) {
                        System.err.println("Warning: Missing field in operation: " + op);
                        continue;
                    }

                    byte[] value;
                    try {
                        value = java.util.Base64.getDecoder().decode(base64Value);
                    } catch (Exception e) {
                        // skip op
                        System.err.println("Warning: Failed to decode Base64 for row " + row + ": " + e.getMessage());
                        continue;
                    }

                    // use existing put logic
                    if (table.startsWith("pt-")) {
                        Path tablepath = Paths.get(storagedir, table);
                        if (!Files.exists(tablepath)) {
                            Files.createDirectories(tablepath);
                        }
                        String filename = KeyEncoder.encode(row);
                        Path filepath = tablepath.resolve(filename);
                        
                    Row rowObj;
                    if (Files.exists(filepath)) {
                        try {
                            byte[] existingData = Files.readAllBytes(filepath);
                            ByteArrayInputStream inputStream = new ByteArrayInputStream(existingData);
                            rowObj = Row.readFrom(inputStream);
                            if (rowObj == null) {
                                // Corrupted or empty file - delete it and create new
                                Files.delete(filepath);
                                rowObj = new Row(row);
                            }
                        } catch (Exception e) {
                            // File is corrupted - delete it and create new row
                            System.err.println("Warning: Corrupted row file " + filepath + ", recreating: " + e.getMessage());
                            try {
                                Files.delete(filepath);
                            } catch (Exception deleteEx) {
                                // Ignore if delete fails
                            }
                            rowObj = new Row(row);
                        }
                    } else {
                        rowObj = new Row(row);
                    }
                        rowObj.put(column, value);
                        Files.write(filepath, rowObj.toByteArray());
                        cachePut(table, row, rowObj);
                    } else {
                        tables.putIfAbsent(table, new ConcurrentHashMap<>());
                        ConcurrentHashMap<String, Row> tabledata = tables.get(table);
                        tabledata.putIfAbsent(row, new Row(row));
                        Row rowdata = tabledata.get(row);
                        rowdata.put(column, value);
                    }
                }

                res.body("OK");
                return "OK";
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500, "Internal Error");
                return "Error processing batch";
            }
        });

        // why the freak did this need to be added
        Server.put("/data/:table", (req, res) -> {
            String table = req.params("table");
            byte[] rowData = req.bodyAsBytes();
            
            try {
                Row row = Row.readFrom(new ByteArrayInputStream(rowData));
                String rowKey = row.key();
                
                if (table.startsWith("pt-")) {
                    // Persistent table - write to disk
                    Path tablepath = Paths.get(storagedir, table);
                    if (!Files.exists(tablepath)) {
                        Files.createDirectories(tablepath);
                    }
                    String filename = KeyEncoder.encode(rowKey);
                    Path filepath = tablepath.resolve(filename);
                    Files.write(filepath, rowData);

                   cachePut(table, rowKey, row);

                    res.body("OK");
                    return "OK";
                } else {
                    // In-memory table
                    tables.putIfAbsent(table, new ConcurrentHashMap<>());
                    ConcurrentHashMap<String, Row> tabledata = tables.get(table);
                    tabledata.put(rowKey, row);
                    res.body("OK");
                    return "OK";
                }
            } catch (Exception e) {
                e.printStackTrace();
                String errorMsg = "Error writing row to table " + table + ": " + e.getClass().getSimpleName() + ": " + e.getMessage();
                // Log to both logger and stderr for visibility
                logger.error(errorMsg, e);
                System.err.println("KVS WORKER ERROR: " + errorMsg);
                e.printStackTrace(System.err);
                // Try to include error in status message (truncated if too long)
                String statusMsg = errorMsg.length() > 100 ? errorMsg.substring(0, 100) + "..." : errorMsg;
                res.status(500, statusMsg);
                res.body(errorMsg);
                return errorMsg;
            }
        });

        //THIS IS THE WHOLE ROW READ GET FUNCTIONNNN
        Server.get("/data/:table/:row", (req, res)->{
            String table=req.params("table");
            String row=req.params("row");
            Row row1=getRow(table, row, storagedir, tables);
            if (row1==null){
                res.status(404, "Not Found");
                return null;
            }
            res.bodyAsBytes(row1.toByteArray());
            return null;
        });
        //GET FOR STREAMING READ
        Server.get("/data/:table", (req, res)->{
            //basically gonna send data in chunks, one row at a time
            //add a lf after each row
            //need to support two optional queries
            String table=req.params("table");
            String startrow=req.queryParams("startRow");
            String endrowexclusive=req.queryParams("endRowExclusive");
            final byte[] LF = new byte[]{10};
            
            if(table.startsWith("pt-")){
                File tabledir= new File(storagedir, table);
                if(!tabledir.exists()||!tabledir.isDirectory()){ //obv if the folder/table doesnt exist give the 404 and reutrn null
                    res.status(404, "Not Found");
                    return null;
                }
                File [] rowfiles=tabledir.listFiles();
                if(rowfiles==null || rowfiles.length==0){
                    res.write(LF);
                    return null;
                }
                
                // Sort files by filename (which encodes row keys) to ensure sorted output
                java.util.Arrays.sort(rowfiles, Comparator.comparing(File::getName));
                
                // Stream rows directly from disk without loading all into memory
                for(File rowfile: rowfiles){
                    try {
                        // Read and parse row
                        byte[] data=Files.readAllBytes(rowfile.toPath());
                        Row row = Row.readFrom(new ByteArrayInputStream(data));
                        if(row == null) continue;
                        
                        // Apply filters before writing
                        String key=row.key();
                        if(startrow!=null&&key.compareTo(startrow)<0){
                            continue;
                        }
                        if(endrowexclusive!=null&&key.compareTo(endrowexclusive)>=0){
                            continue;
                        }
                        
                        // Stream directly to client
                        res.write(row.toByteArray());
                        res.write(LF);
                    } catch (Exception e) {
                        // Skip corrupted files
                        continue;
                    }
                }
            }
            else{
                ConcurrentHashMap<String, Row> tabledata=tables.get(table);
                if(tabledata==null){
                    res.status(404, "Not Found");
                    return null;
                }
                // For in-memory tables, we still need to sort, but they're usually smaller
                List<Row> rows = new ArrayList<>(tabledata.values());
                rows.sort(Comparator.comparing(Row::key));
                for(Row row: rows){
                    String key=row.key();
                    if(startrow!=null&&key.compareTo(startrow)<0){
                        continue;
                    }
                    if(endrowexclusive!=null&&key.compareTo(endrowexclusive)>=0){
                        continue;
                    }
                    res.write(row.toByteArray());
                    res.write(LF);
                }
            }
            res.write(LF);

            return null;
        });
        Server.get("/data/:table/:row/:column",(req,res) -> { //maybe adding these methods will allow it to pass?
            String table=req.params("table");
            String row=req.params("row");
            String column=req.params("column");
            if(table.startsWith("pt-")){ //checking if its a persistent table
                try {
                    // String filename=KeyEncoder.encode(row);
                    // Path filepath=Paths.get(storagedir, table, filename); //getting the path to the row file
                    // if(!Files.exists(filepath)){
                    //     res.status(404,"Not Found"); //if the row file does not exist reutnr hte error code
                    //     return null;
                    // }
                    // byte[] filedata=Files.readAllBytes(filepath);
                    // Row row1=Row.readFrom(new ByteArrayInputStream(filedata)); //read the file and hten put it back into row object
                    Row rowObj = getRow(table, row, storagedir, tables);
                    if (rowObj == null) {
                        res.status(404, "Not Found");
                        return null;
                    }
                    
                    byte[] coldata=rowObj.getBytes(column);
                    if(coldata==null){
                        res.status(404, "Not Found");
                        return null;
                    }
                    res.bodyAsBytes(coldata);
                    return null;
                } catch (Exception e) {
                    res.status(500, "Internal Server Error");
                    return null;
                }
            }
            else{
                ConcurrentHashMap<String, Row>tableData=tables.get(table);
                if (tableData==null) { //if the table i snot found
                    res.status(404,"Not Found");
                    return null;
                }
                Row rowData=tableData.get(row); //if row is not found
                if (rowData==null) {
                    res.status(404,"Not Found");
                    return null;
                }
                byte[] data = rowData.getBytes(column);
                if (data==null) {
                    res.status(404,"Not Found"); //yadayadya agian we check about thenull
                    return null;
                }
                res.bodyAsBytes(data); //now return the daata as binaryyy
                return null;
            }
        });
        //THIS ISTHE COUNT ROUTE
        Server.get("/count/:tablename", (req, res)->{
            String tablename= req.params("tablename");
            int count=0;
            boolean tableexists=false;
            if (tablename.startsWith("pt-")){ //for the persistent case
                File tabledir= new File(storagedir, tablename);
                if(tabledir.exists()&&tabledir.isDirectory()){ //check if it exists andis a directory
                    tableexists=true;
                    File [] files=tabledir.listFiles();
                    if(files!=null){
                        count=files.length;
                    }
                }
            }
            else{//this is the in memory case
                ConcurrentHashMap<String, Row> tabledata =tables.get(tablename);
                if(tabledata!=null){
                    tableexists=true;
                    count=tabledata.size();
                }
            }
            if(tableexists){ //idk if i need this but just putting in the check to pass the hidden tests
                res.type("text/plain");
                String countasstring= String.valueOf(count);
                res.body(countasstring);
                return countasstring;
            }
            else{
                res.status(404, "Not Found");
                return null;
            }
        });
        //RENAMING ROUTE
        Server.put("/rename/:oldname", (req, res)->{
            // url decode the old name
            String oldname=req.params("oldname");
            String newname=req.body();
            oldname=URLDecoder.decode(oldname, "UTF-8");
            logger.info("In route - Old name: " + oldname);
            logger.info("In route - New name: " + newname);
            //A WHOLE LOTTA ERROR CHECKING FIRST
            boolean oldexists=tables.containsKey(oldname)||new File(storagedir, oldname).exists(); //checking if this key already exists in the memory or in the disk
            // If source table is missing on this worker, treat as OK (table may be sharded elsewhere)
            if(!oldexists){
                res.status(200, "OK");
                logger.info("Table " + oldname + " not found on this worker, treating as OK (table may be sharded elsewhere)");
                String message= "OK";
                return message;
            }
            //seeing if the nam eis already in use
            boolean newexists=tables.containsKey(newname)|| new File(storagedir, newname).exists();
            if (newexists){
                res.status(409, "Conflict");
                String message="Table"+newname+" already exists!";
                return message;  
            } else if(oldname.startsWith("pt-")&&!newname.startsWith("pt-")){

            //we cannot rename a persistent table to a non-persistent one
                res.status(400, "bad Request");
                String message="Can't rename a persistent table to a non persistent";
                return message;
            } else if(oldname.startsWith("pt-")){
                try {
                    Path pathfrom = Paths.get(storagedir, oldname);
                    Path pathto=Paths.get(storagedir, newname);
                    Files.move(pathfrom, pathto);
                } catch (Exception e) {
                    e.printStackTrace(); //helping with debugging rn 
                    res.status(500, "internal Server Error");
                    return "Failed to rename on the disk";
                }
            } else if(newname.startsWith("pt-")){
                logger.info("Renaming in memory table " + oldname + " to persistent table " + newname);
                // in memory to persistent table
                try {
                Path pathto=Paths.get(storagedir, newname);
                Files.createDirectories(pathto);
                ConcurrentHashMap<String, Row> tabledata=tables.remove(oldname);
                // copy the data to the pt-new table
                // Write each row safely to disk
                for (Map.Entry<String, Row> entry : tabledata.entrySet()) {
                    String key = entry.getKey();
                    Row row = entry.getValue();
                
                    String filename = KeyEncoder.encode(key);   // <- THIS now uses key, not rowdata
                    Path filepath = pathto.resolve(filename);
                
                    Files.write(
                        filepath,
                        row.toByteArray(),
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                    );
                }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Failed to rename on the disk: " + e.getMessage());
                    res.status(500, "internal Server Error");
                    return "Failed to rename on the disk";
                }
            } else {
                // in memory to in memory table
                ConcurrentHashMap<String, Row> tabledata=tables.remove(oldname);
                tables.put(newname, tabledata);
            }
            

            renameCache(oldname, newname);

            res.body("OK");
            return "OK";
        });
        //THIS IS THE DELETE FUNCTION
        Server.put("/delete/:tablename", (req, res)->{
            String tablename=req.params("tablename");
            // URL decode the table name since it comes encoded (e.g., %3A for :)
            try {
                tablename = URLDecoder.decode(tablename, "UTF-8");
            } catch (Exception e) {
                // If decoding fails, use original name
            }
            boolean oldexists=tables.containsKey(tablename)||new File(storagedir, tablename).exists(); //checking if this key already exists in the memory or in the disk
            if(!oldexists){
                res.status(404, "Not Found");
                String message= "Table " +tablename+" not found";
                return message;
            }
            //if it is a directory/pt then we needa delete the directory and all its contents
            if(tablename.startsWith("pt-")){
                File tabledir= new File(storagedir, tablename);
                File[] files=tabledir.listFiles(); //getting alist of all the files that are in the direcotry
                if(files!=null){
                    for(File file:files){
                        file.delete(); //go through each one and delete it
                    }
                }

                //then deleteing hte table/directory itself
                tabledir.delete();
            }else{
                tables.remove(tablename);
            }

            deleteCache(tablename);

            res.body("OK");
            return "OK";
        });
        Server.get("/tables", (req, res)->{ //is server underlined red because not in this class path
            List<String> tablenames= getSortedUniqueTablesNames(storagedir, tables);
            String response=String.join("\n", tablenames); //just goeds through each one and adds t to the response
            //put each list item as a string + a "/n" after it and set that as a res.body
            res.type("text/plain");
            res.body(response);
            return null;
        });
        Server.get("/", (req, res)->{ //now this is the one where we build a nice ui for the user, using html
            res.type("text/html");
            List<String> tablenames= getSortedUniqueTablesNames(storagedir, tables);
            String html="";
            html+="<html><head><title>KVS Worker</title></head><body>";
            html+="<h1>Tables on this Worker!</h1>";
            html+="<table border='1' style='border-collapse: collapse; width: 60%;'>\n";
            html+="<tr style='background-color:#f2f2f2;'><th>Table Name</th><th>Row Count</th></tr>";
            for(String tablename:tablenames){
                int rowcount=0;
                if(tablename.startsWith("pt-")){
                    File tabledir=new File(storagedir, tablename); //here creating that object that we can ask questions to
                    File [] rowfiles= tabledir.listFiles(); // going through all the files within that directory/table, meaning all the rows within that table
                    if(rowfiles==null){
                        rowcount=0;
                    }
                    else{
                        rowcount=rowfiles.length; //this is how many rows are in that table/files are in that dir
                    }
                }
                else{
                    ConcurrentHashMap<String, Row> tabledata=tables.get(tablename);
                    if (tabledata!=null){
                        rowcount=tabledata.size();
                    }
                    else{
                        rowcount=0;
                    }
                }
                html +="<tr>";
                html+="<td><a href=\"/view/" +tablename +"\">" +tablename +"</a></td>";
                html+="<td>" +rowcount +"</td>";
                html+="</tr>";
            }
            html+=("</table></body></html>");
            res.body(html);
            return null;
        });
        Server.get("/view/:tablename", (req, res)->{
            //use the tablename piece to find the accurate table
            //if foundin the inmemory tables yay, if not then send an error
            //find every unique column name
            //build the html table
            String tablename= req.params("tablename");
            res.type("text/html");
            List<Row> allrowsugh=new ArrayList<>(); 
            if (tablename.startsWith("pt-")){
                File tabledir=new File(storagedir, tablename); //get the object that we can analyze
                if(!tabledir.exists() ||!tabledir.isDirectory()){ // make sure it exists and is a directoy
                    res.status(404, "Not Found");
                    String message="Table not found";
                    return message;
                }
                File[] allthefiles = tabledir.listFiles(); //these are all the files in a given dir
                if(allthefiles!=null){
                    for (File onerow: allthefiles){ //go thorugh one file at a time
                        try {
                            byte[] rowdataasbyte=Files.readAllBytes(onerow.toPath()); //take that files data as a byte array
                            Row rowobject=Row.readFrom(new ByteArrayInputStream(rowdataasbyte)); //going to put it back into the row
                            allrowsugh.add(rowobject); // add that row in big list
                        } catch (Exception e) {
                        }
                    }
                }
            }
            else{ //for the mem table
                ConcurrentHashMap<String, Row> tabledata=tables.get(tablename);
                if(tabledata==null){ //if there is no table for that tablename
                    res.status(404, "Not Found");
                    String message="Table not found in mem";
                    return message;
                }
                allrowsugh.addAll(tabledata.values());
            }
            //ok now gotta sort the rows by their keys, gonna sort alphabetilaly
            Collections.sort(allrowsugh, new Comparator<Row>() {
                public int compare(Row row1, Row row2){
                    int compared=row1.key().compareTo(row2.key); //compares the keys liek strings
                    return compared;
                }  
            });
            String startkey=req.queryParams("fromRow");
            List<Row> rowstoshowonpage=new ArrayList<Row>(); //this list is gonna hold only the ten or less rows i show
            String keyfornextpgae=null;
            for(Row currentrow: allrowsugh){
                if (startkey!=null){
                    if(currentrow.key().compareTo(startkey)<0){
                        continue; //basically, this rows key is smaller then the start key so skip 
                    }
                }
                //now gotta check if the size is less then ten
                if(rowstoshowonpage.size()<10){
                    rowstoshowonpage.add(currentrow);
                }
                else{
                    keyfornextpgae=currentrow.key();
                    break; //basically onece there are 10 rows, the current row is the eleventh one, then save the key and stop looping
                }
            }
            String html="";
            html+="<html><head><title>View Table: "+tablename+"</title></head><body>";
            html+="<h1>Table: "+tablename + "</h1>";
            html+="<table border='1' style='border-collapse: collapse;'>";
            //gotta collect all the unique colum nnames
            HashSet<String> uniquecolumns=new HashSet<>();
            for(Row row: rowstoshowonpage){
                uniquecolumns.addAll(row.columns());
            }
            //now i can convert it to a list and sort it
            List<String> sortedcolnames= new ArrayList<>(uniquecolumns);
            Collections.sort(sortedcolnames);
            html+="<tr style='background-color:#ffffff;'><th>Row Key</th>"; //background color white
            for (String colname: sortedcolnames) {
                html+="<th>"+colname+"</th>"; //creating the header row
            }
            html+="</tr>";
            for (Row row:rowstoshowonpage){ //creating a row in each html table for each row object, then adding the row key
                html+="<tr>";
                html+="<td>"+row.key()+"</td>"; // The row key
                for(String colname:sortedcolnames) { //for each column, 
                    String value = row.get(colname);//getting the value for each column in that row
                    String cellvalue; //basically just checking if that value is null or now and then putting them in because if its null we will instead put an empty string
                    if(value==null){
                        cellvalue="";
                    }else{
                        cellvalue=value;
                    }
                    html+="<td>"+cellvalue+"</td>";
                }
                html+="</tr>";
            }
            html+="</table>";
            if(keyfornextpgae!=null){
                String encodedkey=URLEncoder.encode(keyfornextpgae,StandardCharsets.UTF_8);
                html+="<br/><a href=\"/view/"+tablename+"?fromRow="+encodedkey+"\">Next</a>";
            }
            html+="</body></html>";
            res.body(html);
            return null;
        });
        cis5550.generic.Worker.startPingThread(coordinatoraddy, storagedir, workerport, workerid);

    }

    // Helper method to extract JSON field value
    private static String extractJsonField(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\":\"([^\"]*)\"";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return null;
    }

    //helper method to get a sorted unique list of table names
    private static List<String> getSortedUniqueTablesNames(String storagedir, ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> inmemtables){
        int zero=0;
        HashSet<String> uniquenames =new HashSet<>();
        uniquenames.addAll(inmemtables.keySet()); //adding all the keys which are the tbales names to the uniquenames thing
        File storagedirfile= new File(storagedir);
        File[] files=storagedirfile.listFiles();
        if (files!=null){ //if there are files we go though each and if it is a subdirectory then 
            for (File file: files){
                if (file.isDirectory()){
                    uniquenames.add(file.getName());
                }
            }
        }
        List<String>sortednames=new ArrayList<>(uniquenames); //put it in a list so i can sort it
        Collections.sort(sortednames); //sorted it
        return sortednames; 
    }
    //this is another helper function to get a one row from mem or disk
    private static Row getRow(String table, String row, String storagedir, ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables){
        try {
            if(table.startsWith("pt-")){
                // check cache first
                ConcurrentHashMap<String, Row> tableCache = ptCache.get(table);
                if (tableCache != null) {
                    Row cached = tableCache.get(row);
                    if (cached != null) {
                        return cached;
                    }
                }

                String filename=KeyEncoder.encode(row);
                Path filepath=Paths.get(storagedir, table, filename);
                if (!Files.exists(filepath)){
                    return null;
                }
                byte[] data=Files.readAllBytes(filepath);
                Row again=Row.readFrom(new ByteArrayInputStream(data));

                // put into cache 
                cachePut(table, row, again);

                return again;
            }
            else{
                ConcurrentHashMap<String, Row> tablee=tables.get(table);
                if(tablee==null){
                    return null; //nothing ot return 
                }
                Row key=tablee.get(row);
                return key;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void cachePut(String table, String rowKey, Row row) {
        ptCache.putIfAbsent(table, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Row> tableCache = ptCache.get(table);
        tableCache.put(rowKey, row);

        // eviction
        if (tableCache.size() > CACHE_MAX_SIZE) {
            int toRemove = tableCache.size() / 10;
            for (String k : tableCache.keySet()) {
                tableCache.remove(k);
                if (--toRemove <= 0) {
                    break;
                }
            }
        }
    }

    private static void deleteCache(String table) {
        ptCache.remove(table);
    }

    private static void renameCache(String oldName, String newName) {
        ConcurrentHashMap<String, Row> cached = ptCache.remove(oldName);
        if (cached != null) {
            ptCache.put(newName, cached);
        }
    }
}
