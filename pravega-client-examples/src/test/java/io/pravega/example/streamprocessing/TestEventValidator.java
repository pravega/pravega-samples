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

public class TestEventValidator {
    static final Logger log = LoggerFactory.getLogger(TestEventValidator.class);

    // Map from routing key to highest received sequence number.
    private final Map<Integer, Long> receivedSequenceNumbers = new HashMap<>();
    // Map from instanceId to count of events processed by the instance, excluding duplicates.
    private final Map<Integer, Long> eventCountByInstanceId = new HashMap<>();
    private long duplicateEventCount;

    public void validate(Iterator<TestEvent> events, Map<Integer, Long> expectedLastSequenceNumbers) {
        // pendingSequenceNumbers contains a map from key to sequence number for events that have been generated but not yet received by validate.
        // A key is removed when all events up to the generated sequence number for that key are received.
        final Map<Integer, Long> pendingSequenceNumbers = new HashMap<>();
        expectedLastSequenceNumbers.forEach((key, sequenceNumber) -> {
            if (receivedSequenceNumbers.getOrDefault(key, -1L) < sequenceNumber) {
                pendingSequenceNumbers.put(key, sequenceNumber);
            }
        });
        log.info("pendingSequenceNumbers={}", pendingSequenceNumbers);
        while (events.hasNext()) {
            final TestEvent event = events.next();
            final long lastReceivedSequenceNumber = receivedSequenceNumbers.getOrDefault(event.key, -1L);
            log.info("event={}, lastReceivedSequenceNumber={}", event, lastReceivedSequenceNumber);
            if (event.sequenceNumber <= lastReceivedSequenceNumber) {
                log.warn("Duplicate event; event={}, lastReceivedSequenceNumber={}", event, lastReceivedSequenceNumber);
                duplicateEventCount++;
            } else if (event.sequenceNumber > lastReceivedSequenceNumber + 1) {
                throw new MissingEventException("Detected missing event");
            } else {
                receivedSequenceNumbers.put(event.key, event.sequenceNumber);
                eventCountByInstanceId.merge(event.processedByInstanceId, 1L, Long::sum);   // increment counter
                if (pendingSequenceNumbers.getOrDefault(event.key, -1L) <= event.sequenceNumber) {
                    pendingSequenceNumbers.remove(event.key);
                    if (pendingSequenceNumbers.size() == 0) {
                        log.info("All data received; receivedSequenceNumbers={}", receivedSequenceNumbers);
                        return;
                    }
                }
            }
        }
        throw new NoMoreEventsException("No more events but all expected events were not received");
    }

    public void clearCounters() {
        eventCountByInstanceId.clear();
        duplicateEventCount = 0;
    }

    public Map<Integer, Long> getEventCountByInstanceId() {
        return eventCountByInstanceId;
    }

    public long getDuplicateEventCount() {
        return duplicateEventCount;
    }
}
