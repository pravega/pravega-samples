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

public class SampleEvent {
    public long sequenceNumber;
    public String routingKey;
    public long intData;
    public long sum;
    public long timestamp;
    public String timestampStr;
    public long processedLatencyMs;
    public String processedBy;

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
