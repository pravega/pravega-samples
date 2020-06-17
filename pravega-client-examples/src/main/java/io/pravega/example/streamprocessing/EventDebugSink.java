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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * A simple example that continuously shows the events in a stream.
 */
public class EventDebugSink {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EventDebugSink.class);

    private static final int READER_TIMEOUT_MS = 2000;

    private final AppConfiguration config;

    public static void main(String[] args) throws Exception {
        EventDebugSink app = new EventDebugSink(new AppConfiguration(args));
        app.run();
    }

    public EventDebugSink(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() throws Exception {
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(getConfig().getControllerURI()).build();
        try (StreamManager streamManager = StreamManager.create(getConfig().getControllerURI())) {
            streamManager.createScope(getConfig().getScope());
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            getConfig().getTargetRateEventsPerSec(),
                            getConfig().getScaleFactor(),
                            getConfig().getMinNumSegments()))
                    .build();
            streamManager.createStream(getConfig().getScope(), getConfig().getStream2Name(), streamConfig);
        }

        // Create a reader group that begins at the earliest event.
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(getConfig().getScope(), getConfig().getStream2Name()))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(getConfig().getScope(), getConfig().getControllerURI())) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(getConfig().getScope(), clientConfig);
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
            finally {
                readerGroupManager.deleteReaderGroup(readerGroup);
            }
        }
    }
}
