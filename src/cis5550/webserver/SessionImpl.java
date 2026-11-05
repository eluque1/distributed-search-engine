package cis5550.webserver;

import cis5550.webserver.Session;

import java.security.SecureRandom;
import java.util.*;

public class SessionImpl implements Session {
    private String sessionID = "";
    private long creationTime;
    private long lastAccessedTime;
    private int tmax = 300000;
    private boolean isValid = true;
    private HashMap<String, Object> sessionMap = new HashMap<String, Object>();
    private static final String characters =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-:";
    private static final SecureRandom randomGenerator = new SecureRandom();

    SessionImpl() {
        creationTime = System.currentTimeMillis();
        lastAccessedTime = System.currentTimeMillis();
        this.isValid = true;
    }

    @Override
    public String id() {
        if (sessionID.isEmpty()) {
            StringBuilder sb = new StringBuilder(20);
            for (int i = 0; i < 20; i++) {
                int index = randomGenerator.nextInt(characters.length());
                sb.append(characters.charAt(index));
            }
            this.sessionID = sb.toString();
        }
        return sessionID;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        tmax = seconds * 1000;
    }

    @Override
    public void invalidate() {
        this.isValid = false;
    }

    @Override
    public Object attribute(String name) {
        return sessionMap.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        sessionMap.put(name, value);
    }

    public void updateLastAccessedTime(long time) {
        this.lastAccessedTime = time;
    }

    public int tmax() {
        return tmax;
    }

    public boolean isValid() {
        return this.isValid;
    }

}
