package cis5550.webserver;

import cis5550.tools.Logger;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

class RequestImpl implements Request {
    private static final Logger logger = Logger.getLogger(Server.class);
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String,String> headers;
    Map<String,String> queryParams;
    Map<String,String> params;
    byte bodyRaw[];
    Server server;
    SessionImpl session;
    String newSessionID = null;


    RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg,
                Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg,
                byte bodyRawArg[], Server serverArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        server = serverArg;
    }

    public String requestMethod() {
      return method;
    }
    public void setParams(Map<String,String> paramsArg) {
      params = paramsArg;
    }
    public int port() {
      return remoteAddr.getPort();
    }
    public String url() {
      return url;
    }
    public String protocol() {
      return protocol;
    }
    public String contentType() {
      return headers.get("content-type");
    }
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }
    public String body() {
      return new String(bodyRaw, StandardCharsets.UTF_8);
    }
    public byte[] bodyAsBytes() {
      return bodyRaw;
    }
    public int contentLength() {
      return bodyRaw.length;
    }
    public String headers(String name) {
      return headers.get(name.toLowerCase());
    }
    public Set<String> headers() {
      return headers.keySet();
    }
    public String queryParams(String param) {
      return queryParams.get(param);
    }
    public Set<String> queryParams() {
      return queryParams.keySet();
    }
    public String params(String param) {
      return params.get(param);
    }
    public Map<String,String> params() {
      return params;
    }
    public Session session() {

        // first, look for a session id in the request headers
        // logger.error("checking for session id in request headers");
        String sessionID = "";
        String cookieHeader = headers.get("cookie");
        if (cookieHeader != null) {
            String[] pairs = cookieHeader.split(";");
            for (String pair : pairs) {
                String[] keyVal = pair.split("=");
                if (keyVal[0].equalsIgnoreCase("sessionid")) {
                  sessionID = keyVal[1];
                }
            }
        }

        // now check if that session id corresponds to a session in the server
        // we create a new session with a new id in the cases:
        // no cookie header
        // no id in the cookie header
        // id is not actually associated with a session
        // if there is a session associated with an id given in cookie header, then just return that session
        SessionImpl currentSession = server.getSession(sessionID);
        if (currentSession != null && currentSession.isValid()) {
            long now = System.currentTimeMillis();
            long downtime = (now - currentSession.lastAccessedTime());
            if (downtime > currentSession.tmax()) {
                logger.error("invalidated in session() call at " + System.currentTimeMillis());
                currentSession.invalidate();
                server.removeSession(sessionID, currentSession);
                currentSession = null;
                sessionID = "";

            }
        }
        //TODO: could make session invalidation/removal more optimized but not sure how

        // if session is not null and we didnt just set it to invalid
        if (currentSession != null && currentSession.isValid()) {
            logger.error("found session in server session map, id is: " + sessionID);
            this.session = currentSession;
            session.updateLastAccessedTime(System.currentTimeMillis());
            return currentSession;
        } else {
            logger.error("did not find session, creating now");
            this.session = new SessionImpl();
            sessionID = session.id();
            // updating the map in the server
            // logger.error("updating server session map");
            logger.error("this is the session id we created: "+ sessionID);
            server.setSession(sessionID, session);
            this.newSessionID = sessionID;
            return session;
        }
    }

    public String getNewSessionID() {
        return this.newSessionID;
    }

    public boolean sessionIsValid() {
        return this.session.isValid();
    }


}
