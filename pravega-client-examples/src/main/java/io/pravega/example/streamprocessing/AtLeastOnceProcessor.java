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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

abstract public class AtLeastOnceProcessor implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceProcessor.class);

    private final ReaderGroup readerGroup;
    private final String membershipSynchronizerStreamName;
    private final Serializer<String> serializer;
    private final ReaderConfig readerConfig;
    private final EventStreamClientFactory eventStreamClientFactory;
    private final SynchronizerClientFactory synchronizerClientFactory;
    private final ScheduledExecutorService executor;
    private final long heartbeatIntervalMillis;
    private final long readTimeoutMillis;

    public AtLeastOnceProcessor(ReaderGroup readerGroup, String membershipSynchronizerStreamName, Serializer<String> serializer, ReaderConfig readerConfig, EventStreamClientFactory eventStreamClientFactory, SynchronizerClientFactory synchronizerClientFactory, ScheduledExecutorService executor, long heartbeatIntervalMillis, long readTimeoutMillis) {
        this.readerGroup = readerGroup;
        this.membershipSynchronizerStreamName = membershipSynchronizerStreamName;
        this.serializer = serializer;
        this.readerConfig = readerConfig;
        this.eventStreamClientFactory = eventStreamClientFactory;
        this.synchronizerClientFactory = synchronizerClientFactory;
        this.executor = executor;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    @Override
    public Void call() throws Exception {
        final String readerId = UUID.randomUUID().toString();
        try (final ReaderGroupPruner pruner = ReaderGroupPruner.create(
                readerGroup,
                membershipSynchronizerStreamName,
                readerId,
                synchronizerClientFactory,
                executor,
                heartbeatIntervalMillis)) {
            try (final EventStreamReader<String> reader = eventStreamClientFactory.createReader(
                    readerId,
                    readerGroup.getGroupName(),
                    serializer,
                    readerConfig)) {
                for (; ; ) {
                    final EventRead<String> eventRead = reader.readNextEvent(readTimeoutMillis);
                    log.info("call: eventRead={}", eventRead);
                    if (eventRead.isCheckpoint()) {
                        flush(eventRead);
                    } else if (eventRead.getEvent() != null) {
                        write(eventRead);
                    }
                }
            }
        }
    }

    abstract public void write(EventRead<String> eventRead);

    public void flush(EventRead<String> eventRead) {
    }
}
