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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;

/**
 * Object to process Apache access log
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessLog {
    private String ClientIP;

    private String Status;

    private String Verb;

    private String TimestampStr;

    public AccessLog(){
        Status=Verb=ClientIP="";
    }

    @JsonProperty("clientip")
    public String getClientIP() {
        return ClientIP;
    }

    public void setClientIP(String clientIP) {
        ClientIP = clientIP;
    }

    @JsonProperty("response")
    public String getStatus() {
        return Status;
    }

    public void setStatus(String status) {
        Status = status;
    }

    @JsonProperty("@timestamp")
    public String getTimestampStr() { return TimestampStr; }

    public void setTimestampStr(String timestampStr) { TimestampStr = timestampStr; }

    public long getTimestampMillis() {
        return new DateTime(getTimestampStr()).getMillis();
    }

    @JsonProperty("verb")
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
               accessLog.TimestampStr.equals(TimestampStr) &&
               accessLog.ClientIP.equals(ClientIP);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((Status == null) ? 0 : Status.hashCode());
        result = prime * result + ((ClientIP == null) ? 0 : ClientIP.hashCode());
        result = prime * result + ((TimestampStr == null) ? 0 : TimestampStr.hashCode());
        result = prime * result + ((Verb == null) ? 0 : Verb.hashCode());
        return result;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "AccessLog: Timestamp=" + getTimestampStr() +", ClientIP=" + getClientIP() + ", Verb=" + getVerb() + ", Status=" + getStatus();
        }
    }
}
