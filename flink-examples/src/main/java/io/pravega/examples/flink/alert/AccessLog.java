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

import io.pravega.shaded.com.google.type.Date;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Object to process Apache access log
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessLog {
    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("clientip")
    private String clientIp;

    @JsonProperty("response")
    private String status;

    @JsonProperty("verb")
    private String verb;

    @JsonProperty("@timestamp")
    private String timestamp;

    public static AccessLog toAccessLog(String value) throws IOException {
        return mapper.readValue(value, AccessLog.class);
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getStatus()
    {
        return status;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestampStr) {
        this.timestamp = timestampStr;
    }

    public long getTimestampMillis()
    {
        return new DateTime(getTimestamp()).getMillis();
    }

    public String getVerb()
    {
        return verb;
    }

    public void setVerb(String verb)
    {
        this.verb = verb;
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
        return accessLog.verb.equals(verb) &&
               accessLog.status.equals(status) &&
               accessLog.timestamp.equals(timestamp) &&
               accessLog.clientIp.equals(clientIp);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result + ((clientIp == null) ? 0 : clientIp.hashCode());
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((verb == null) ? 0 : verb.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "AccessLog [timestamp = "+timestamp+", verb = "+verb+", status = "+status+", clientIp = "+clientIp+"]";
    }
}
