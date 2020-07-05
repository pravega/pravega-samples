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
    public long sequenceNumber;
    public String routingKey;

    public TestEvent(long sequenceNumber, String routingKey) {
        this.sequenceNumber = sequenceNumber;
        this.routingKey = routingKey;
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "sequenceNumber=" + String.format("%6d", sequenceNumber) +
                ", routingKey=" + routingKey +
                '}';
    }
}
