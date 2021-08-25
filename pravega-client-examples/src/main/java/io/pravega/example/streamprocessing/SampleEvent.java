/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.streamprocessing;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SampleEvent {
    final public long sequenceNumber;
    final public String routingKey;
    final public long intData;
    final public long sum;
    final public long timestamp;
    final public String timestampStr;
    final public long processedLatencyMs;
    final public String processedBy;

    static final private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    /// Create a new SampleEvent that represents an unprocessed event.
    public SampleEvent(long sequenceNumber, String routingKey, long intData, long sum) {
        this.sequenceNumber = sequenceNumber;
        this.routingKey = routingKey;
        this.intData = intData;
        this.sum = sum;
        this.timestamp = System.currentTimeMillis();
        this.timestampStr = dateFormat.format(new Date(this.timestamp));
        this.processedLatencyMs = 0;
        this.processedBy = null;
    }

    /// Create a new SampleEvent that represents a processed event.
    public SampleEvent(SampleEvent event, String processedBy) {
        this.sequenceNumber = event.sequenceNumber;
        this.routingKey = event.routingKey;
        this.intData = event.intData;
        this.sum = event.sum;
        this.timestamp = event.timestamp;
        this.timestampStr = event.timestampStr;
        this.processedLatencyMs = System.currentTimeMillis() - event.timestamp;
        this.processedBy = processedBy;
    }

    @Override
    public String toString() {
        return "SampleEvent{" +
                "sequenceNumber=" + String.format("%6d", sequenceNumber) +
                ", routingKey=" + routingKey +
                ", intData=" + String.format("%3d", intData) +
                ", sum=" + String.format("%8d", sum) +
                ", timestampStr=" + timestampStr +
                ", processedLatencyMs=" + String.format("%6d", processedLatencyMs) +
                ", processedBy=" + processedBy +
                '}';
    }
}
