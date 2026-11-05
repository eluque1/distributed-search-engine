// okay i am going to go through and add comments on all my code so that i remember what i did
//making it a package and importing relevant things
package cis5550.generic;
import cis5550.webserver.Request;
import cis5550.webserver.Response;
import cis5550.webserver.Server;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//This entire file is about the coordinator, who's main job is to keep a list of workers, the ones that are active, and direct a client to the right worker
public class Coordinator {
    // we have to use a concurrent hash map instead of a normal one so that multiple threads can edit it safely
    //this map will have the key as the id and then the value as the worker entry, gonna start off empty
    public static final ConcurrentHashMap<String, WorkerEntry> activeWorkers = new ConcurrentHashMap<>();
    //this is how long a worker is going to be inactive until it times out, 15000 milliseconds is 15 seconds
    private static final long workertimeoutTIME=15000;

    //this is method one in which i return the current list of workers as ip: port strings
    public static List<String> getWorkers(){
        //here i call a helper function sortedworkers, and basically returns a list of the active workers sorted by id
        List<WorkerEntry> sortedworkers = sortedWorkers();
        List<String> workeraddresses = new ArrayList<>();
        //basically making the list of the addresses by doing the ip + the port and adding it to a list
        for (WorkerEntry worker: sortedworkers){
            String addy=worker.ip+":"+worker.port;
            workeraddresses.add(addy);
        }
        return workeraddresses;
    }
    //this is the code that build an html table of all the active workers
    public static String workerTable(){
        //again the active workers are called by the sorted workers which just filters all the workers keeping only those that are NOT expeired and then sorting them by id
        List<WorkerEntry> activeworkers= sortedWorkers();
        //this is just the html code which is creating a tbale with the columns id, ip:port, and link
        String wTable="<h2>Active Workers (real)</h2>"+ "<table border=\"1\"><tr><th>ID</th><th>IP:Port</th><th>Link</th></tr>";
        //this is if ther are no acive workers then we can just say that in the table
        if(activeworkers.isEmpty()){
            wTable+="<tr><td colspan=\"2\">No active workers as of now.</td></tr>";
        }
        //if there is something in the active worker list (which to remind is the a list of worker entry objects (id, ip-port, last ping time))
        else{
            //we go thorugh every worker and create a string of the ip port for the url
            for(WorkerEntry worker: activeworkers){
                String ipport=worker.ip+":"+worker.port;
                //this makes the url link
                String url="http://"+ipport+"/";
                wTable+="<tr>"; //this makes a table row
                wTable+="<td>" + worker.id + "</td>"; //this is a table data cell with the id
                wTable+= "<td>"+"<a href=\"" + url + "\">" + ipport + "</a>"+"</td>"; //this makes the link
                wTable+= "</tr>"; //this is another row
            }
        }
        wTable+="</table>"; //ended the table lolz
        // so here we say that the worker table was called and we created it with x number o active workers
        System.err.println("workerTable() called: " + activeworkers.size() + " active workers");
        //this is for my debugging, jsut printing the the active workers, their id,ip, port and the last ping time
        for (WorkerEntry w : activeworkers) {
            System.err.println("  -> " + w.id + " at " + w.ip + ":" + w.port + " (lastPing=" + w.lastPingTime + ")");
        }
        //returning the htlm table
        return wTable;
    }
    
    // clientTable is an alias for workerTable (used by Flame coordinator)
    public static String clientTable() {
        return workerTable();
    }
    
    //
    public static void registerRoutes(){
        Server.get("/workers", (Request req, Response res) ->{
            List<WorkerEntry> sortedworkers=sortedWorkers();
            int k=sortedworkers.size();
            String result=k+"\n";
            for(WorkerEntry worker: sortedworkers){
                result=result+worker.id + ","+worker.ip+":"+worker.port+"\n";
            }
            res.type("text/plain");
            res.body(result);
            return null;
        });
        //when a worker sends a /ping?id= etc. etc. then the following runs
        Server.get("/ping",(Request req, Response res) -> {
            String id= req.queryParams("id");
            String port= req.queryParams("port");
            long currenttime=System.currentTimeMillis(); //this is the current time
            if (id==null||port == null) {
                res.status(400, "Bad Request");
                res.body("ID and port parameters are neeeded");
                return null;
            }
            String ip = req.ip(); //this is the ip address of the worker
            activeWorkers.compute(id, (key, worker) -> { 
                if (worker==null) {
                    worker=new WorkerEntry(id, ip, port); //here if the workerentry value does not exist we create it
                    worker.lastPingTime=currenttime;
                    System.err.println("Registere worker: " +id + " at " +ip+":"+port);
                } 
                else {
                    worker.ip =ip;
                    worker.port =port;
                    worker.lastPingTime =currenttime; //this is the crux of it, just changing the lastPingTime to now to basically keep it alive
                }

                return worker;
            });
            res.status(200, "OK");
            res.body("OK"); //yay it worked
            System.err.println("PING RECEIVED: id=" + id + " ip=" + ip + " port=" + port);
            System.err.println("Map size after update = " +activeWorkers.size());

            return null;
        });
    }
    //returns a list of worker entry objects
    private static List<WorkerEntry> sortedWorkers(){
        //this would be a list of all the values in active workers which means each elemtn in this is a worker Entry object
        List<WorkerEntry> allWorkers=new java.util.ArrayList<>(activeWorkers.values());
        //current time duh
        long currenttime=System.currentTimeMillis();
        //active workers is an empty list currently
        List<WorkerEntry> activeworkers = new ArrayList<>();
        //we go through each worker in my all worker list
        for (WorkerEntry worker: allWorkers){
            long timesincelastping=currenttime-worker.lastPingTime;
            //we compare the last ping time to our curren time and if it not expired then we add it
            if(timesincelastping<=workertimeoutTIME){
                activeworkers.add(worker);
            }
            else{
                activeWorkers.remove(worker.id);
            }
        }
        //sorting the list by id lexographically which is ABCD instead of something else, this is helpeful later on
        Collections.sort(activeworkers, (w1, w2) ->  w1.id.compareTo(w2.id)); //basicallu just sorting the list by id
        return activeworkers;
        
    }
}
