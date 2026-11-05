package cis5550.kvs;
import cis5550.webserver.Server;
import cis5550.webserver.Response;
import cis5550.webserver.Request;
//import cis5550.generic.Coordinator;
//import cis5550.kvs.KVSClient.WorkerEntry;

import java.util.concurrent.*;

public class Coordinator extends cis5550.generic.Coordinator{
    //i wanna parse in the command line arguments
    //then pass the port number into the webservers port
    //then call register routes
    //then define a route for / that does a few things
    public static void main(String[] args){
        int port = 0;
        if(args.length<1 || args.length>1){
            System.err.println("wrong number of command line arguments");
            System.exit(1);
        }
        try{
            port = Integer.parseInt(args[0]);
        }
        catch(NumberFormatException e){
            System.err.println("the port argument must be a valid int");    //see why i have an infinite loop!
            System.exit(1);
        }
        Server.port(port); //tells the webserver to listen on that port
        Coordinator coord=new Coordinator(); // start a new cooridnator
        coord.registerRoutes(); // calls register routes
        Server.get("/", (Request req, Response res) ->{ // this is deifning the home page route so without the rest of it, basically printing the worker table
            String workerTableHtml = coord.workerTable();
            String html = "<html><head><title>KVS Coordinator</title></head><body>";
            html = html + "<h1>KVS Coordinator Status</h1>";
            html = html + workerTableHtml;
            html = html + "</body></html>";
            res.type("text/html");
            res.body(html);
            return null;
        });
        System.err.println("KVS coord running on port " +port );

    }
}
