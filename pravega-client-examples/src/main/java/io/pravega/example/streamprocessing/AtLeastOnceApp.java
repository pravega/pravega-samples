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

import com.google.gson.reflect.TypeToken;
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
import org.slf4j.Logger;
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
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceApp.class);
    
    private final AppConfiguration config;

    public static void main(String[] args) throws Exception {
        final AtLeastOnceApp app = new AtLeastOnceApp(new AppConfiguration(args));
        app.run();
    }

    public AtLeastOnceApp(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }
    
    public void run() throws Exception {
        // Get the provided instanceId that uniquely identifes this instances of AtLeastOnceApp.
        // It will be randomly generated if not provided by the user.
        final String instanceId = getConfig().getInstanceId();
        log.info("instanceId={}", instanceId);

        // Define configuration to connect to Pravega.
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(getConfig().getControllerURI()).build();

        // Create the input and output streams (ignored if they already exist).
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
            // Create the Reader Group (ignored if it already exists)
            readerGroupManager.createReaderGroup(getConfig().getReaderGroup(), readerGroupConfig);
            final ReaderGroup readerGroup = readerGroupManager.getReaderGroup(getConfig().getReaderGroup());
            try (EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(getConfig().getScope(), clientConfig);
                 SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(getConfig().getScope(), clientConfig);
                 // Create a Pravega stream writer that we will send our processed output to.
                 EventStreamWriter<SampleEvent> writer = eventStreamClientFactory.createEventWriter(
                         getConfig().getStream2Name(),
                         new JSONSerializer<>(new TypeToken<SampleEvent>(){}.getType()),
                         EventWriterConfig.builder().build())) {

                final AtLeastOnceProcessor<SampleEvent> processor = new AtLeastOnceProcessor<SampleEvent>(
                        instanceId,
                        readerGroup,
                        getConfig().getMembershipSynchronizerStreamName(),
                        new JSONSerializer<>(new TypeToken<SampleEvent>(){}.getType()),
                        ReaderConfig.builder().build(),
                        eventStreamClientFactory,
                        synchronizerClientFactory,
                        Executors.newScheduledThreadPool(1),
                        getConfig().getHeartbeatIntervalMillis(),
                        1000) {

                    /**
                     * Process an event that was read.
                     * Processing can be performed asynchronously after this method returns.
                     * This method must be stateless.
                     *
                     * For this demonstration, we output the same event that was read but with
                     * the processedBy field set.
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
                };

                processor.startAsync();

                // Add shutdown hook for graceful shutdown.
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    log.info("Running shutdown hook.");
                    processor.stopAsync();
                    log.info("Waiting for processor to terminate.");
                    processor.awaitTerminated();
                    log.info("Processor terminated.");
                }));

                processor.awaitTerminated();
            }
        }
    }
}
