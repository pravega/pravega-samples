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
import io.pravega.client.stream.EventStreamReader;
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

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * A simple example that demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 *
 * This runs only a single thread.
 *
 * Use {@link EventGenerator} to generate input events and {@link EventDebugSink}
 * to view the output events.
 *
 * See {@link ExactlyOnceMultithreadedProcessor} for an improved version.
 */
public class AtLeastOnceProcessorMain {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AtLeastOnceProcessorMain.class);

    private static final int READER_TIMEOUT_MS = 2000;

    private final String scope;
    private final String readerGroupName;
    private final String inputStreamName;
    private final String outputStreamName;
    private final URI controllerURI;

    public AtLeastOnceProcessorMain(String scope, String readerGroupName, String inputStreamName, String outputStreamName, URI controllerURI) {
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.inputStreamName = inputStreamName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
    }

    public static void main(String[] args) throws Exception {
        AtLeastOnceProcessorMain processor = new AtLeastOnceProcessorMain(
                Parameters.getScope(),
                Parameters.getStream1Name() + "-rg",
                Parameters.getStream1Name(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI());
        processor.run();
    }

    public void run() throws Exception {
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        final String membershipSynchronizerStreamName = readerGroupName + "-membership";
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
            streamManager.createStream(
                    scope,
                    membershipSynchronizerStreamName,
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }
        final String readerGroupName = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, inputStreamName))
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
                        500,
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
