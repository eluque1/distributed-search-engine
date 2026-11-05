package cis5550.webserver;

import cis5550.tools.Logger;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;



public class Server {
    public static class staticFiles{
        public static void location(String s) {
            Server.location = s;
        }
    }
    static class RouteEntry {
        String pathPattern;
        Route handler;
        RouteEntry(String pathPattern, Route handler) {
            this.pathPattern = pathPattern;
            this.handler = handler;
        }
    }
    private static final Logger logger = Logger.getLogger(Server.class);
    private static final int NUM_WORKERS = 100;
    private static Server server = null;
    private static boolean flag = false;
    private static String location;
    private static int port = 80;
    private static String currentHost = "UNDEFINED";
    private static ServerSocket serverSocketTLS = null;
    private static Map<String, List<RouteEntry>> routeTable = new HashMap<String, List<RouteEntry>>();
    private static java.util.concurrent.ConcurrentHashMap<String, SessionImpl> sessionMap =
            new java.util.concurrent.ConcurrentHashMap<>();


    public static void port(int portNum) {
        Server.port = portNum;
    }
    public static void host(String hostname) {
        currentHost = hostname;
    }

    public static void get(String pathPattern, Route route) {
        if (server == null) {
            server = new Server();
        }
        if (!flag) {
            flag = true;
            new Thread(Server::run).start();
        }

        List<RouteEntry> tableEntry = routeTable.get("GET");
        if (tableEntry != null) {
            tableEntry.add(new RouteEntry(pathPattern, route));
        } else {
            List<RouteEntry> list = new ArrayList<RouteEntry>();
            list.add(new RouteEntry(pathPattern, route));
            routeTable.put("GET", list);
        }
    }

    public static void put(String pathPattern, Route route) {
        if (server == null) {
            server = new Server();
        }
        if (!flag) {
            flag = true;
            new Thread(Server::run).start();
        }
        List<RouteEntry> tableEntry = routeTable.get("PUT");
        if (tableEntry != null) {
            tableEntry.add(new RouteEntry(pathPattern, route));
        } else {
            List<RouteEntry> list = new ArrayList<RouteEntry>();
            list.add(new RouteEntry(pathPattern, route));
            routeTable.put("PUT", list);
        }
    }

    public static void post(String pathPattern, Route route) {
        if (server == null) {
            server = new Server();
        }
        if (!flag) {
            flag = true;
            new Thread(Server::run).start();
        }
        List<RouteEntry> tableEntry = routeTable.get("POST");
        if (tableEntry != null) {
            tableEntry.add(new RouteEntry(pathPattern, route));
        } else {
            List<RouteEntry> list = new ArrayList<RouteEntry>();
            list.add(new RouteEntry(pathPattern, route));
            routeTable.put("POST", list);
        }
    }

    public static void securePort(int port) throws Exception {
        String pwd = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        ServerSocketFactory factory = sslContext.getServerSocketFactory();
        serverSocketTLS = factory.createServerSocket(port);

    }

    public static void run() {
        int portNum;
        String fileDirectory;
        BlockingQueue<Socket> queue = new LinkedBlockingQueue<Socket>();
        try {
            portNum = port;
            fileDirectory = location;
        } catch (Exception exception) {
            System.out.println("Written by Anjana Begur");
            return;
        }

        // starts the threads, which will be waiting for a socket to be added to the queue
        for (int i = 0; i < NUM_WORKERS; i++) {
                Thread worker = new Thread(() -> {
                    while (true) {
                        try {
                            Socket client = queue.take();
                            handleAllRequests(client, fileDirectory);
                        } catch (Exception e) {
                            logger.error("Exception while waiting for client", e);
                        }
                    }
                });
                worker.start();
        }

        Thread sessionExpiryChecker = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();

                    for (String sessionID : sessionMap.keySet()) {
                        SessionImpl session = sessionMap.get(sessionID);
                        if (!session.isValid()) {
                            sessionMap.remove(sessionID, session);
                        }
                        long downtime = (now - session.lastAccessedTime());
                        logger.error("Session check - now: " + now + ", lastAccessed: " + session.lastAccessedTime() + ", downtime: " + downtime + ", tmax: " + session.tmax());
                        if (downtime > session.tmax()) {
                            logger.error("Session expired, invalidating");
                            session.invalidate();
                            sessionMap.remove(sessionID, session);
                        } else {
                            logger.error("session still valid");
                        }
                    }
                } catch (Exception e) {
                    logger.error("Exception in session expiration checker", e);
                }

            }
        });
        sessionExpiryChecker.start();

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(portNum);
        } catch (Exception e) {
            logger.error("Error in creating http ServerSocket", e);
            return;
        }
        ServerSocket finalServerSocket = serverSocket;
        Thread httpListener = new Thread(() -> acceptLoop(finalServerSocket, queue));
        httpListener.start();
        if (serverSocketTLS != null) {
            Thread httpsListener = new Thread(() -> acceptLoop(serverSocketTLS, queue));
            httpsListener.start();
        }

    }
    private static void acceptLoop(ServerSocket serverSocket, BlockingQueue<Socket> queue) {
        try {
            while (true) {
                Socket client = serverSocket.accept();
                queue.put(client);
            }
        } catch (Exception e) {
            logger.error("Listener stopped", e);
        }
    }

    private static void handleAllRequests(Socket client, String fileDirectory) {
        while (true) {
            try {
                InputStream inputStream = client.getInputStream();
                OutputStream outputStream = client.getOutputStream();
                boolean shouldContinue = handleClient(client, fileDirectory, inputStream, outputStream);

                if (!shouldContinue) {
                    break;
                }

            } catch (Exception e) {
                logger.error("Error in handleAllClients", e);
            }
        }

        try {
            client.close();
        } catch (Exception e) {
            logger.error("Error in client closing", e);
        }

    }

    private static boolean handleClient(Socket client, String fileDirectory, InputStream inputStream, OutputStream outputStream) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            int b;
            byte[] end = {13, 10, 13, 10};
            int matches = 0;
            try {
                while ((b = inputStream.read()) != -1) {
                    bytes.write(b);
                    if (b == end[matches]) {
                        matches++;
                        if (matches == 4) {
                            break;
                        }
                    } else {
                        if (b == end[0]) {
                            matches = 1;
                        } else {
                            matches = 0;
                        }
                    }
                }


            } catch (SocketException se) {
                logger.info("Client disconnected while reading request");
                return false;
            }

            if (b == -1 && bytes.size() == 0) {
                // logger.info("Received empty request");
                return false;
            }

            // writing from byte array to output
            byte[] headerBytes = bytes.toByteArray();
            ByteArrayInputStream baiInputStream = new ByteArrayInputStream(headerBytes);
            BufferedReader reader = new BufferedReader(new InputStreamReader(baiInputStream));

            String line = reader.readLine();
            String firstLine = line;
            int contentLength = 0;
            String requestcType = "";
            boolean hostFound = false;
            String modifiedSinceHeader = "";
            String rangeHeader = "";
            String cookieHeader = "";

            if (firstLine == null) {
                sendError(client, 400, "Bad Request");
                return true;
            }

            Map<String, String> headersMap = new HashMap<String, String>();
            while (line != null) {
                if (line.isEmpty()) {
                    break; // end of headers
                }
                if (line.toLowerCase().startsWith("content-length: ")) {
                    contentLength = Integer.parseInt(line.toLowerCase().substring("content-length:".length()).trim());
                } else if (line.toLowerCase().startsWith("host: ")) {
                    hostFound = true;
                } else if (line.toLowerCase().startsWith("if-modified-since: ")) {
                    modifiedSinceHeader = line.toLowerCase().substring("if-modified-since:".length()).trim();
                } else if (line.toLowerCase().startsWith("range: ")) {
                    rangeHeader = line.toLowerCase().substring("range:".length()).trim();
                } else if (line.toLowerCase().startsWith("content-type: ")) {
                    requestcType = line.toLowerCase().substring("content-type:".length()).trim();
                } else if (line.toLowerCase().startsWith("cookie: ")) {
                    cookieHeader = line.toLowerCase().substring("cookie:".length()).trim();
                }
                int i = line.indexOf(":");
                if (i != -1) {
                    String key = line.substring(0, i).trim().toLowerCase();
                    String value = line.substring(i + 1).trim();
                    headersMap.put(key, value);
                }
                line = reader.readLine();
            }

            // parsing session id from cookie header + updating session if it exists
            String sessionID = "";
            HashMap <String, String> cookiesMap = new HashMap<String, String>();
            if (!cookieHeader.isEmpty()) {
                String[] pairs = cookieHeader.split(";");
                for (String pair : pairs) {
                    String[] keyVal = pair.split("=");
                    // Only process if we have both key and value
                    if (keyVal.length >= 2) {
                        cookiesMap.put(keyVal[0].trim(), keyVal[1].trim());
                        if (keyVal[0].trim().toLowerCase().equals("sessionid")) {
                            sessionID = keyVal[1].trim();
                        }
                    }
                }
            }

            if (sessionMap.containsKey(sessionID)) {
                SessionImpl session = sessionMap.get(sessionID);
                if (session.isValid()) {
                    long now = System.currentTimeMillis();
                    long downtime = (now - session.lastAccessedTime()) / 1000;
                    if (downtime > session.tmax()) {
                        // logger.error("second check for validity");
                        session.invalidate();
                        sessionMap.remove(sessionID);
                    } else {
                        session.updateLastAccessedTime(now);
                    }
                }

            }

            // reading content from body
            // Safety check: reject requests larger than 100MB to prevent OOM
            final int MAX_CONTENT_LENGTH = 100 * 1024 * 1024; // 100MB
            if (contentLength > MAX_CONTENT_LENGTH) {
                sendError(client, 413, "Request Entity Too Large (max " + (MAX_CONTENT_LENGTH / 1024 / 1024) + "MB)");
                return true;
            }
            
            byte[] bodyContent = new byte[contentLength];
            if (contentLength > 0) {
                int total = 0;
                while (total < contentLength) {
                    int bytesRead = inputStream.read(bodyContent, total, contentLength - total);
                    if (bytesRead == -1) {
                        break;
                    }
                    total += bytesRead;
                }
            }

            // ERROR HANDLING

            if (!hostFound) {
                sendError(client, 400, "Bad Request");
                logger.info("Received malformed request");
                return true;
            }
            String[] firstLineWords = firstLine.split(" ");
            if (firstLineWords.length != 3) {
                sendError(client, 400, "Bad Request");
                logger.info("Received malformed request");
                return true;
            }
            String method = firstLineWords[0];
            String url = firstLineWords[1];
            String version = firstLineWords[2];


            if (!method.equals("HEAD") && !method.equals("GET") && !method.equals("POST") && !method.equals("PUT")) {
                sendError(client, 501, "Not Implemented");
                return true;
            }
            if (!version.equals("HTTP/1.1")) {
                sendError(client, 505, "HTTP Version Not Supported");
                return true;
            }


            // response handling - if there is a registered route
            HashMap<String, String> qParamsMap = new HashMap<String, String>();
            if (url.contains("?")) {
                String rawParams = url.split("\\?")[1];
                parseQParams(qParamsMap, rawParams);
            } if (requestcType.equals("application/x-www-form-urlencoded")) {
                String rawParams =  new String(bodyContent, StandardCharsets.UTF_8);
                parseQParams(qParamsMap, rawParams);
            }
            boolean handledDynamically = false;
            ResponseImpl response = null;
            Object handlerResult = null;
            HashMap<String, String> params = new HashMap<String, String>();
            RequestImpl request = null;
            if (routeTable.containsKey(method)) {
                List<RouteEntry> entryList = routeTable.get(method);
                for (RouteEntry entry : entryList) {
                    String pathOnly = url.split("\\?")[0];
                    if (matchesPattern(pathOnly, entry.pathPattern, params)) {
                        handledDynamically = true;
                        InetSocketAddress remote = (InetSocketAddress) client.getRemoteSocketAddress();
                        request = new RequestImpl(
                                method,
                                url,
                                "HTTP/1.1",
                                headersMap,
                                qParamsMap,
                                params,
                                remote,
                                bodyContent,
                                server
                        );
                        response = new ResponseImpl(outputStream);
                        try {
                            handlerResult = entry.handler.handle(request, response);
                            break;
                        } catch (Exception e) {
                            if (!response.isWriteCalled()) {
                                sendError(client, 500, "Internal Server Error");
                            } else {
                                client.close();
                                return false;
                            }
                        }
                    }
                }
            }

            // constructing response - either handled dynamically if route is registered or serving static file
            if (handledDynamically) {
                if (!response.isWriteCalled()) {
                    byte[] responseBody = response.getBody();
                    String contentType = response.getContentType();
                    int statusCode = response.getStatusCode();
                    String reasonPhrase = response.getReasonPhrase();

                    if (contentType == null) contentType = "text/html";
                    if (statusCode == 0) statusCode = 200;
                    if (reasonPhrase == null) reasonPhrase = "OK";

                    StringBuilder headers = new StringBuilder();
                    headers.append("HTTP/1.1 ").append(statusCode).append(" ").append(reasonPhrase).append("\r\n");
                    headers.append("Content-Type: ").append(contentType).append("\r\n");
                    headers.append("Server: Anjana's Server\r\n");
                    headers.append("Connections:close\r\n");

                    if (!response.isWriteCalled()) {
                        if (handlerResult != null) {
                            responseBody = handlerResult.toString().getBytes(StandardCharsets.UTF_8);
                        } else if (response.getBody() != null) {
                            responseBody = response.getBody();
                        }
                    }

                    if (responseBody != null) {
                        headers.append("Content-Length: ").append(responseBody.length).append("\r\n");
                    } else {
                        headers.append("Content-Length: ").append(0).append("\r\n");
                    }
                    for (Map.Entry<String, String> h : response.getHeaders().entrySet()) {
                        headers.append(h.getKey()).append(": ").append(h.getValue()).append("\r\n");
                    }
                    if (request.getNewSessionID() != null) {
                        headers.append("Set-Cookie: SessionID=").append(request.getNewSessionID()).append("\r\n");
                    }
                    headers.append("\r\n");
                    outputStream.write(headers.toString().getBytes("UTF-8"));

                    if (responseBody != null) {
                        outputStream.write(responseBody);
                    }
                    outputStream.flush();
                } else {
                    return false;
                }
            } else { // if there is no registered route, see if static file exists as fallback
                if (location == null) {
                    sendError(client,404, "File not found");
                    return true;
                }
                if (method.equals("POST") || method.equals("PUT")) {
                    sendError(client, 405,"Not Allowed");
                    return true;
                }
                File file = fetchFile(client, fileDirectory, url);
                if (file == null) {
                    return true;
                }

                String lastModifiedHeader = "";
                if (!modifiedSinceHeader.isEmpty()) {
                    DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
                    try {
                        ZonedDateTime ifModifiedSince = ZonedDateTime.parse(modifiedSinceHeader, formatter);
                        long lastModifiedMS = Files.getLastModifiedTime(file.toPath()).toMillis();
                        ZonedDateTime lastModified = Instant.ofEpochMilli(lastModifiedMS).atZone(ZoneOffset.UTC);
                        lastModifiedHeader = formatter.format(lastModified);
                        if (!ifModifiedSince.isAfter(ZonedDateTime.now(ZoneOffset.UTC))) {
                            if (!lastModified.toInstant().isAfter(ifModifiedSince.toInstant())) {
                                sendError(client, 304, "Not Modified");
                                return true;
                            }
                        }
                    } catch (DateTimeParseException e) {
                        logger.info("Error occurred during if-modified-since check");
                    }
                }


                // writing response
                int extStart = file.getName().lastIndexOf(".");
                String extension;
                String fileType = "";
                if (extStart != -1) {
                    extension = file.getName().substring(extStart + 1);
                    if (extension.equals("jpg") || extension.equals("jpeg")) {
                        fileType = "image/jpeg";
                    } else if (extension.equals("txt")) {
                        fileType = "text/plain";
                    } else if (extension.equals("html")) {
                        fileType = "text/html";
                    }
                } else {
                    fileType = "application/octet-stream";
                }

                boolean rangeSent = false;
                if (!rangeHeader.isEmpty() && method.equals("GET")) {
                    rangeSent = handleRangeRequest(rangeHeader, file, fileType, outputStream);
                }
                if (rangeSent) {
                    return true;
                }

                String headers =
                        "HTTP/1.1 200 OK\r\n" +
                                "Content-Type:" + fileType + "\r\n" +
                                "Server: Anjana's Server\r\n" +
                                "Content-Length: " + file.length() + "\r\n" +
                                "Last-Modified: " + lastModifiedHeader + "\r\n" +
                                "\r\n";
                outputStream.write(headers.getBytes("UTF-8"));
                outputStream.flush();

                // printing file contents
                if (file.length() != 0 && !method.equals("HEAD")) {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        try {
                            while ((bytesRead = fis.read(buffer)) != -1) {
                                outputStream.write(buffer, 0, bytesRead);
                            }
                            outputStream.flush();
                        } catch (SocketException se) {
                            logger.info("Client disconnected while sending response");
                            return false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Exception in client handling", e);
            return false;
        }
        // keep connection open
        return true;
    }

    private static File fetchFile(Socket client, String fileDirectory, String url) {
        if (url.startsWith("/")) {
            url = url.substring(1);
        }
        String fullPath = fileDirectory + "/" + url;
        try {
            if (fullPath.contains("..")) {
                sendError(client, 403, "Forbidden");
            }

            try {
                File file = new File(fullPath);
                if (!file.exists()) {
                    sendError(client, 404, "Not Found");
                    return null;
                }
                return file;
            } catch (SecurityException e) {
                sendError(client, 403, "Forbidden");
            }
        } catch (Exception e) {
            logger.error("Exception in file handling", e);
        }
        return null;
    }

    private static void sendError(Socket client, int code, String message) throws IOException {
        // maybe put try catch
        String errorBody = code + " " + message;
        PrintWriter out = new PrintWriter(client.getOutputStream());
        out.print("HTTP/1.1 " + code + " " + message + "\r\n");
        out.print("Content-Length: " + errorBody.length() + "\r\n");
        out.print("\r\n");
        out.print(errorBody);
        out.flush();
    }

    private static boolean handleRangeRequest(String rangeHeader, File file, String fileType, OutputStream outputStream) {
        try {
            if (!rangeHeader.startsWith("bytes=")) {
                return false;
            }
            String range = rangeHeader.substring(6).trim();
            if (!range.contains("-")) {
                return false;
            }

            long rangeStart = -1;
            long rangeEnd = -1;
            if (range.startsWith("-")) {
                rangeEnd = file.length() - 1;
                rangeStart = file.length() - Long.parseLong(range.substring(1).trim());
            } else if (range.endsWith("-")) {
                rangeStart = Long.parseLong(range.substring(0, range.length() - 1));
                rangeEnd = file.length() - 1;
            } else {
                String[] endpoints = range.split("-");
                if (endpoints.length != 2) {
                    return false;
                }
                rangeStart = Long.parseLong(endpoints[0].trim());
                rangeEnd = Long.parseLong(endpoints[1].trim());
            }

            RandomAccessFile raf = new RandomAccessFile(file, "r");
            long contentLength = (rangeEnd - rangeStart) + 1;
            // logger.info("Content length: " + contentLength);

            String headers =
                    "HTTP/1.1 206 Partial Content\r\n" +
                            "Content-Type: " + fileType + "\r\n" +
                            "Content-Length: " + contentLength + "\r\n" +
                            "Content-Range: bytes " + rangeStart + "-" + rangeEnd + "/" + file.length() + "\r\n" +
                            "\r\n";


            outputStream.write(headers.getBytes("UTF-8"));
            raf.seek(rangeStart);
            byte[] buffer = new byte[8192];
            long bytesLeft = contentLength;
            while (bytesLeft > 0) {
                int bytesToRead = (int)Math.min(buffer.length, bytesLeft);
                int bytesRead = raf.read(buffer, 0, bytesToRead);
                if (bytesRead == -1) break;
                outputStream.write(buffer, 0, bytesRead);
                bytesLeft -= bytesRead;
            }

            outputStream.flush();
            raf.close();

            return true;
        } catch (Exception e) {
            logger.info("Error in range check" + e);
            return false;
        }
    }

    private static boolean matchesPattern(String url, String pathPattern, HashMap<String, String> params) {
        String[] urlList = url.split("/");
        String[] pathList = pathPattern.split("/");

        params.clear();

        if (urlList.length != pathList.length) {
            return false;
        }
        for (int i = 0; i < urlList.length; i++) {
            if (pathList[i].startsWith(":")) {
                params.put(pathList[i].substring(1), urlList[i]);
            }
            if (!(pathList[i].equals(urlList[i])) && !pathList[i].startsWith(":")) {
                return false;
            }
        }
        return true;
    }

    private static void parseQParams(HashMap<String,String> qParamsMap, String rawParams) {
        String[] pairs = rawParams.split("&");
        try {
            for (String pair : pairs) {
                String[] separatedPair = pair.split("=", 2);
                String key = URLDecoder.decode(separatedPair[0], StandardCharsets.UTF_8);
                String value = "";
                if (separatedPair.length > 1) {
                    value = URLDecoder.decode(separatedPair[1], StandardCharsets.UTF_8);
                }
                qParamsMap.put(key, value);
            }
        } catch (Exception e) {
            logger.error("Invalid query parameter form", e);
        }

    }

    public SessionImpl getSession(String sessionID) {
        return sessionMap.get(sessionID);
    }

    public void setSession(String sessionID, SessionImpl session) {
        sessionMap.put(sessionID, session);
    }
    public void removeSession(String sessionID, SessionImpl session) {
        sessionMap.remove(sessionID, session);
    }

}