package cis5550.generic; //part of generic
import java.io.InputStream;
import java.io.OutputStream; //this and the input stream etc. is all the read and write data
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

//this is the worker side of that communication, specifically the ping stuff
public class Worker {
    public static void startPingThread(String coordinatoraddy, String storagedir, int workerport, String workerid){
        String finalworkerid=workerid;
        String portstring=String.valueOf(workerport);
        Thread pingthread=new Thread(()->{
            while(!Thread.currentThread().isInterrupted()){ //point is it keeps running indefintely, unless its interuppte
                try {
                    String pingurl="http://"+coordinatoraddy+"/ping?id="+finalworkerid+"&port="+portstring; //builds url
                    URL url=new URL(pingurl);
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection(); //opens onnection
                    conn.setConnectTimeout(2000); 
                    conn.setReadTimeout(2000);
                    conn.setRequestMethod("GET");
                    conn.connect();
                    int responseCode=conn.getResponseCode();
                    if (responseCode == 200){ //meaning if connection was made
                        try (InputStream is=conn.getInputStream()) {
                            is.transferTo(OutputStream.nullOutputStream()); //read and discard the output stream
                        }
                        System.err.println("Ping successful to "+pingurl);
                    } 
                    else {
                        try (InputStream es = conn.getErrorStream()) { //if the response code wasnt 200, get the error message
                            if (es != null) {
                                es.transferTo(OutputStream.nullOutputStream());} //read and discard it
                        }
                        System.err.println("Ping failed with code: "+responseCode + " for " + pingurl); //say there was an error
                    }
                    conn.disconnect(); //then we close the connection
                } 
                catch ( Exception e) { //basically if anythig goes wrong for example coordinaor is down
                    System.err.println("ping to coordinator failed"+e.getMessage());
                }
                try {
                    Thread.sleep(5000); // milliseconds to second, wait five secnods and then try again
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); //if the trhead is interrupted , it leaves the loop
                    break;
                }
            }
        }, "Worker-ping-thread");
        pingthread.setDaemon(true); //basically if the program stops, this threaed wont cause issues??im actually begging please work and autograder please please work
        pingthread.start(); //starts running it

    }
    
    // Overloaded method for Flame workers (3 parameters: coordinator address, port string, port int)
    public static void startPingThread(String coordinatorAddress, String portStr, int port) {
        // Generate a random worker ID (5 lowercase letters)
        String workerId = generateRandomId();
        startPingThread(coordinatorAddress, "", port, workerId);
    }
    
    private static String generateRandomId() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            sb.append((char)('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}
