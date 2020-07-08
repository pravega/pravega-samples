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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * This is an abstract class for implementing a stateless event processor with Pravega.
 * It reads an event from a Pravega stream and then calls a user-defined function to process it.
 * It guarantees that each event is processed at least once, even if failures occur.
 * If multiple instances are executed using the same readerGroup parameter,
 * each instance will get a distinct subset of events.
 * Instances can be in different processes.
 */
abstract public class AtLeastOnceProcessor<T> extends AbstractExecutionThreadService {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceProcessor.class);

    private final Supplier<ReaderGroupPruner> prunerSupplier;
    private final Supplier<EventStreamReader<T>> readerSupplier;
    private final long readTimeoutMillis;

    public AtLeastOnceProcessor(Supplier<ReaderGroupPruner> prunerSupplier, Supplier<EventStreamReader<T>> readerSupplier, long readTimeoutMillis) {
        this.prunerSupplier = prunerSupplier;
        this.readerSupplier = readerSupplier;
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
    protected void run() throws Exception {
        // It is critical that the ReaderGroupPruner is running (and therefore has added itself to the membership synchronizer)
        // before the EventStreamReader is created. Otherwise, another ReaderGroupPruner instance may place this reader offline.
        // It is also critical that when this method stops running the ReaderGroupPruner is eventually stopped so that
        // it no longer sends heartbeats.
        try (final ReaderGroupPruner pruner = prunerSupplier.get();
             final EventStreamReader<T> reader = readerSupplier.get()) {
            while (isRunning()) {
                final EventRead<T> eventRead = reader.readNextEvent(readTimeoutMillis);
                log.info("eventRead={}", eventRead);
                // We must inject the fault between read and process.
                // This ensures that a *new* event cannot be processed after the fault injection latch is set.
                injectFault(pruner);
                if (eventRead.isCheckpoint()) {
                    flush();
                } else if (eventRead.getEvent() != null) {
                    process(eventRead);
                }
            }
            // Gracefully stop.
            // Call readNextEvent to indicate that the previous event was processed.
            // When the reader is closed, it will call readerOffline with the proper position.
            log.info("Stopping");
            reader.readNextEvent(0);
            flush();
        }
        log.info("Stopped");
    }

    /**
     * Process an event that was read.
     * Processing can be performed asynchronously after this method returns.
     * This method must be stateless.
     *
     * @param eventRead The event read.
     */
    abstract public void process(EventRead<T> eventRead);

    /**
     * If {@link #process} did not completely process prior events, it must do so before returning.
     * If writing to a Pravega stream, this should call {@link EventStreamWriter#flush}.
     */
    public void flush() {
    }

    protected void injectFault(ReaderGroupPruner pruner) throws Exception {
    }
}
