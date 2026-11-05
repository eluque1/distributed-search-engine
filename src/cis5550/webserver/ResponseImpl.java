package cis5550.webserver;

import cis5550.tools.Logger;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Set;

public class ResponseImpl implements Response {

    private boolean writeCalled = false;
    private HashMap<String, String> headersMap = new HashMap<String, String>();

    private static final Logger logger = Logger.getLogger(Server.class);

    private String contentType = "text/html";
    private int statusCode;
    private String reasonPhrase;
    private byte[] body;
    private OutputStream out;
    private boolean headersWritten = false;

    ResponseImpl(OutputStream out) {
        this.out = out;
    }

    ResponseImpl() {
        this.out = null;
    }

    public HashMap<String, String> getHeaders() {
        return headersMap;
    }

    public boolean isWriteCalled() {
        return writeCalled;
    }

    public byte[] getBody() {
        return body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getContentType() {
        return contentType;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    @Override
    public void body(String body) {
        if (writeCalled) {
            return;
        }
        this.body = body.getBytes();
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (writeCalled) {
            return;
        }
        this.body = bodyArg;

    }

    @Override
    public void header(String name, String value) {
        if (name.equalsIgnoreCase("content type")) {
            return;
        }
        headersMap.put(name,value);
    }

    @Override
    public void type(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (writeCalled) {
            return;
        }
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;

    }

    @Override
    public void write(byte[] b) throws Exception {
        this.writeCalled = true;
        if (!headersWritten) {

            if (contentType == null) contentType = "text/html";
            if (statusCode == 0) statusCode = 200;
            if (reasonPhrase == null) reasonPhrase = "OK";

            StringBuilder headers = new StringBuilder();
            headers.append("HTTP/1.1 ").append(statusCode).append(" ").append(reasonPhrase).append("\r\n");
            headers.append("Content-Type: ").append(contentType).append("\r\n");
            headers.append("Server: Anjana's Server\r\n");

            for (String key : headersMap.keySet()) {
                headers.append(key).append(": ").append(headersMap.get(key)).append("\r\n");
            }

            headers.append("Connection: close\r\n");
            headers.append("\r\n");
            if (out == null) {
                logger.error("outputStream passed to write is null");
            }
            out.write(headers.toString().getBytes("UTF-8"));
            this.headersWritten = true;
        }
        out.write(b);
        out.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (isWriteCalled()) {
            return;
        }
        Set<Integer> validCodes = Set.of(301, 302, 303, 307, 308);
        if (!validCodes.contains(responseCode)) {
            logger.error("invalid response code given for redirect");
            return;
        }
        this.statusCode = responseCode;
        headersMap.put("Location", url);
        try {
            this.writeCalled = true;
            this.write(new byte[0]);
        } catch (Exception e) {
            logger.error("error in url redirect", e);
        }
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {

    }
}
