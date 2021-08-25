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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An Iterator that generates TestEvent elements.
 * Events will have routing keys between 0 and numKeys - 1.
 * For each routing key, sequence numbers will begin at 0 and increment by 1.
 */
public class TestEventGenerator implements Iterator<TestEvent> {
    static final Logger log = LoggerFactory.getLogger(TestEventGenerator.class);

    private final int numKeys;
    private int lastKey;
    // map from routing key to sequence number
    private final Map<Integer, Long> lastSequenceNumbers;

    public TestEventGenerator(int numKeys) {
        this.numKeys = numKeys;
        this.lastKey = numKeys - 1;
        lastSequenceNumbers = new HashMap<>();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public TestEvent next() {
        lastKey = (lastKey + 1) % numKeys;;
        final long sequenceNumber = lastSequenceNumbers.getOrDefault(lastKey, -1L) + 1;
        lastSequenceNumbers.put(lastKey, sequenceNumber);
        final TestEvent event = new TestEvent(lastKey, sequenceNumber);
        log.info("event={}", event);
        return event;
    }

    public Map<Integer, Long> getLastSequenceNumbers() {
        return new HashMap<>(lastSequenceNumbers);
    }
}
