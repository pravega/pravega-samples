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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is an abstract class for implementing a stateless event processor with Pravega.
 * It reads an event from a Pravega stream and then calls a user-defined function to process it.
 * It guarantees that each event is processed at least once, even if failures occur.
 * If multiple instances are executed using the same readerGroup parameter,
 * each instance will get a distinct subset of events.
 * Instances can be in different processes.
 */
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

    public AtLeastOnceProcessor(
            ReaderGroup readerGroup,
            String membershipSynchronizerStreamName,
            Serializer<String> serializer,
            ReaderConfig readerConfig,
            EventStreamClientFactory eventStreamClientFactory,
            SynchronizerClientFactory synchronizerClientFactory,
            ScheduledExecutorService executor,
            long heartbeatIntervalMillis,
            long readTimeoutMillis) {
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

    /**
     * Run the event processor loop.
     *
     * If the previous call to readNextEvent returned a checkpoint, the next call
     * to readNextEvent will record in the reader group that this reader
     * has read and processed events up to the previous {@link Position}.
     */
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
                        process(eventRead);
                    }
                }
            }
        }
    }

    /**
     * Process an event that was read.
     * Processing can be performed asynchronously after this method returns.
     *
     * @param eventRead The event read.
     */
    abstract public void process(EventRead<String> eventRead);

    /**
     * This will be called when a checkpoint event is received.
     * If {@link #process} did not completely process prior events, it must do so now.
     * If writing to a Pravega stream, this should call {@link EventStreamWriter#flush}.
     *
     * @param eventRead Identifies the checkpoint name. This can generally be ignored.
     */
    public void flush(EventRead<String> eventRead) {
    }
}
