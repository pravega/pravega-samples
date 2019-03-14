/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.streamprocessing;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;

/**
 * A simple example that continuously shows the events in a stream.
 *
 * This reads the output of {@link ExactlyOnceMultithreadedProcessor}.
 */
public class EventDebugSink {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EventDebugSink.class);

    private static final int READER_TIMEOUT_MS = 2000;

    public final String scope;
    public final String inputStreamName;
    public final URI controllerURI;

    public EventDebugSink(String scope, String inputStreamName, URI controllerURI) {
        this.scope = scope;
        this.inputStreamName = inputStreamName;
        this.controllerURI = controllerURI;
    }

    public static void main(String[] args) throws Exception {
        EventDebugSink processor = new EventDebugSink(
                Parameters.getScope(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI());
        processor.run();
    }

    public void run() throws Exception {
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            Parameters.getTargetRateEventsPerSec(),
                            Parameters.getScaleFactor(),
                            Parameters.getMinNumSegments()))
                    .build();
            streamManager.createStream(scope, inputStreamName, streamConfig);
        }

        // Create a reader group that begins at the earliest event.
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, inputStreamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamReader<String> reader = clientFactory.createReader(
                     "reader",
                     readerGroup,
                     new UTF8StringSerializer(),
                     ReaderConfig.builder().build())) {
            long eventCounter = 0;
            long sum = 0;
            for (;;) {
                EventRead<String> eventRead = reader.readNextEvent(READER_TIMEOUT_MS);
                if (eventRead.getEvent() != null) {
                    eventCounter++;
                    String[] cols = eventRead.getEvent().split(",");
                    long intData = Long.parseLong(cols[3]);
                    sum += intData;
                    log.info("eventCounter={}, sum={}, event={}",
                            String.format("%06d", eventCounter),
                            String.format("%08d", sum),
                            eventRead.getEvent());
                }
            }
        }
    }
}
