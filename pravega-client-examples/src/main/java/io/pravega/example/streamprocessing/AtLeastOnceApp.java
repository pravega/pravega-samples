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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.net.URI;
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

    private final String scope;
    private final String readerGroupName;
    private final String membershipSynchronizerStreamName;
    private final String inputStreamName;
    private final String outputStreamName;
    private final URI controllerURI;
    private final long heartbeatIntervalMillis;

    public AtLeastOnceApp(String scope, String readerGroupName, String membershipSynchronizerStreamName,
                          String inputStreamName, String outputStreamName, URI controllerURI,
                          long heartbeatIntervalMillis) {
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.membershipSynchronizerStreamName = membershipSynchronizerStreamName;
        this.inputStreamName = inputStreamName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    public static void main(String[] args) throws Exception {
        AtLeastOnceApp processor = new AtLeastOnceApp(
                Parameters.getScope(),
                Parameters.getReaderGroup(),
                Parameters.getMembershipSynchronizerStreamName(),
                Parameters.getStream1Name(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI(),
                Parameters.getHeartbeatIntervalMillis());
        processor.run();
    }

    public void run() throws Exception {
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            streamManager.createScope(scope);
            final StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            Parameters.getTargetRateEventsPerSec(),
                            Parameters.getScaleFactor(),
                            Parameters.getMinNumSegments()))
                    .build();
            streamManager.createStream(scope, inputStreamName, streamConfig);
            streamManager.createStream(scope, outputStreamName, streamConfig);
            // Create stream for the membership state synchronizer.
            streamManager.createStream(
                    scope,
                    membershipSynchronizerStreamName,
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, inputStreamName))
                .automaticCheckpointIntervalMillis(Parameters.getCheckpointPeriodMs())
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
            final ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
            try (EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig)) {
                final AtLeastOnceProcessor processor = new AtLeastOnceProcessor(
                        readerGroup,
                        membershipSynchronizerStreamName,
                        new UTF8StringSerializer(),
                        ReaderConfig.builder().build(),
                        eventStreamClientFactory,
                        synchronizerClientFactory,
                        Executors.newScheduledThreadPool(1),
                        heartbeatIntervalMillis,
                        1000) {
                    @Override
                    public void write(EventRead<String> eventRead) {
                    }
                };
                processor.call();
            }
        }
    }
}
