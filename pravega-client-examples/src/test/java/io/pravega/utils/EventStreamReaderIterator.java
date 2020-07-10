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
package io.pravega.utils;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;

/**
 * An Iterator for reading from a Pravega stream.
 */
public class EventStreamReaderIterator<T> implements Iterator<T> {
    static final Logger log = LoggerFactory.getLogger(EventStreamReaderIterator.class);

    private final EventStreamReader<T> reader;
    private final long timeoutMillis;
    private Optional<T> nextEvent = Optional.empty();

    public EventStreamReaderIterator(EventStreamReader<T> reader, long timeoutMillis) {
        this.reader = reader;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean hasNext() {
        readIfNeeded();
        return nextEvent.isPresent();
    }

    @Override
    public T next() {
        readIfNeeded();
        if (nextEvent.isPresent()) {
            final T event = nextEvent.get();
            nextEvent = Optional.empty();
            return event;
        } else {
            throw new RuntimeException("Timeout");
        }
    }

    private void readIfNeeded() {
        log.info("readIfNeeded: BEGIN");
        if (!nextEvent.isPresent()) {
            final long t0 = System.nanoTime();
            long nextTimeoutMillis = timeoutMillis;
            while (nextTimeoutMillis >= 0) {
                log.info("readIfNeeded: nextTimeoutMillis={}", nextTimeoutMillis);
                final EventRead<T> eventRead = reader.readNextEvent(nextTimeoutMillis);
                log.info("readIfNeeded: eventRead={}", eventRead);
                if (!eventRead.isCheckpoint()) {
                    if (eventRead.getEvent() != null) {
                        nextEvent = Optional.of(eventRead.getEvent());
                    }
                    return;
                }
                nextTimeoutMillis = timeoutMillis - (System.nanoTime() - t0) / 1000 / 1000;
            }
        }
        log.info("readIfNeeded: END");
    }
}
