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
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

/**
 * This demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 * It guarantees that each event is processed at least once.
 * If multiple instances of this application are executed using the same readerGroupName parameter,
 * each instance will get a distinct subset of events.
 *
 * Use {@link EventGenerator} to generate input events and {@link EventDebugSink}
 * to view the output events.
 */
public class AtLeastOnceApp {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AtLeastOnceApp.class);
    
    private final AppConfiguration config;

    public static void main(String[] args) throws Exception {
        AtLeastOnceApp app = new AtLeastOnceApp(new AppConfiguration(args));
        app.run();
    }

    public AtLeastOnceApp(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }
    
    public void run() throws Exception {
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(getConfig().getControllerURI()).build();
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            streamManager.createScope(getConfig().getScope());
            final StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            getConfig().getTargetRateEventsPerSec(),
                            getConfig().getScaleFactor(),
                            getConfig().getMinNumSegments()))
                    .build();
            streamManager.createStream(getConfig().getScope(), getConfig().getStream1Name(), streamConfig);
            streamManager.createStream(getConfig().getScope(), getConfig().getStream2Name(), streamConfig);
            // Create stream for the membership state synchronizer.
            streamManager.createStream(
                    getConfig().getScope(),
                    getConfig().getMembershipSynchronizerStreamName(),
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(getConfig().getScope(), getConfig().getStream1Name()))
                .automaticCheckpointIntervalMillis(getConfig().getCheckpointPeriodMs())
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(getConfig().getScope(), clientConfig)) {
            readerGroupManager.createReaderGroup(getConfig().getReaderGroup(), readerGroupConfig);
            final ReaderGroup readerGroup = readerGroupManager.getReaderGroup(getConfig().getReaderGroup());
            try (EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(getConfig().getScope(), clientConfig);
                 SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(getConfig().getScope(), clientConfig);
                 EventStreamWriter<String> writer = eventStreamClientFactory.createEventWriter(
                         getConfig().getStream2Name(),
                         new UTF8StringSerializer(),
                         EventWriterConfig.builder().build())) {

                final AtLeastOnceProcessor processor = new AtLeastOnceProcessor(
                        readerGroup,
                        getConfig().getMembershipSynchronizerStreamName(),
                        new UTF8StringSerializer(),
                        ReaderConfig.builder().build(),
                        eventStreamClientFactory,
                        synchronizerClientFactory,
                        Executors.newScheduledThreadPool(1),
                        getConfig().getHeartbeatIntervalMillis(),
                        1000) {
                    @Override
                    public void process(EventRead<String> eventRead) {
                        writer.writeEvent("0", "processed," + eventRead.getEvent());
                    }

                    @Override
                    public void flush() {
                        writer.flush();
                    }
                };

                processor.startAsync();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("Running shutdown hook.");
                    processor.stopAsync();
                }));
                processor.awaitTerminated();
            }
        }
    }
}
