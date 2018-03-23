/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 */
package io.pravega.examples.flink.alert;

import io.pravega.shaded.com.google.gson.Gson;

/**
 * Object to process Apache access log
 */
public class AccessLog {
    private String ClientIP;
    private String Status;
    private long Timestamp;
    private String Verb;

    public AccessLog(){
        Status=Verb=ClientIP="";
        Timestamp=0L;
    }

    public String getClientIP() {
        return ClientIP;
    }

    public void setClientIP(String clientIP) {
        ClientIP = clientIP;
    }

    public String getStatus() {
        return Status;
    }

    public void setStatus(String status) {
        Status = status;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.Timestamp = timestamp;
    }

    public String getVerb() {
        return Verb;
    }

    public void setVerb(String verb) {
        Verb = verb;
    }

    /** 
     * The events in the DataStream to which you want to apply pattern matching must
     * implement proper equals() and hashCode() methods because these are used for 
     * comparing and matching events.
     */
    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }
        if(!(obj instanceof AccessLog)){
            return false;
        }
        AccessLog accessLog =(AccessLog)obj;
        return accessLog.Verb.equals(Verb) &&
               accessLog.Status.equals(Status) &&
               accessLog.Timestamp==Timestamp &&
               accessLog.ClientIP.equals(ClientIP);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((Status == null) ? 0 : Status.hashCode());
        result = prime * result + (int) (Timestamp ^ (Timestamp >>> 32));
        result = prime * result + ((ClientIP == null) ? 0 : ClientIP.hashCode());
        result = prime * result + ((Verb == null) ? 0 : Verb.hashCode());
        return result;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
