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

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * This is an implementation of AtLeastOnceProcessor that performs a simple
 * operation to demonstrate processing a SampleEvent.
 * For this demonstration, we output to a Pravega stream the same event that was read but with
 * the processedBy field set.
 */
public class SampleEventProcessor extends AtLeastOnceProcessor<SampleEvent> {
    private static final Logger log = LoggerFactory.getLogger(SampleEventProcessor.class);

    private final String instanceId;
    private final EventStreamWriter<SampleEvent> writer;

    public SampleEventProcessor(Supplier<ReaderGroupPruner> prunerSupplier,
                                Supplier<EventStreamReader<SampleEvent>> readerSupplier,
                                long readTimeoutMillis,
                                String instanceId,
                                EventStreamWriter<SampleEvent> writer) {
        super(prunerSupplier, readerSupplier, readTimeoutMillis);
        this.instanceId = instanceId;
        this.writer = writer;
    }

    /**
     * Process an event that was read.
     * Processing can be performed asynchronously after this method returns.
     * This method must be stateless.
     *
     * @param eventRead The event read.
     */
    @Override
    public void process(EventRead<SampleEvent> eventRead) {
        final SampleEvent event = eventRead.getEvent();
        event.processedBy = instanceId;
        event.processedLatencyMs = System.currentTimeMillis() - event.timestamp;
        log.info("{}", event);
        writer.writeEvent(event.routingKey, event);
    }

    /**
     * If {@link #process} did not completely process prior events, it must do so before returning.
     * If writing to a Pravega stream, this should call {@link EventStreamWriter#flush}.
     */
    @Override
    public void flush() {
        writer.flush();
    }
}
