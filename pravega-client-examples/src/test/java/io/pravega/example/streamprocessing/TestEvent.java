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

public class TestEvent {
    public int key;
    public long sequenceNumber;
    public int processedByInstanceId;

    public TestEvent(int key, long sequenceNumber) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.processedByInstanceId = -1;
    }

    public TestEvent(int key, long sequenceNumber, int processedByInstanceId) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.processedByInstanceId = processedByInstanceId;
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "key=" + key +
                ", sequenceNumber=" + sequenceNumber +
                ", processedByInstanceId=" + processedByInstanceId +
                '}';
    }
}
