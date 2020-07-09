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
import com.google.gson.reflect.TypeToken;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.util.ReusableLatch;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@Builder
public class WorkerProcess extends AbstractExecutionThreadService {
    private static final Logger log = LoggerFactory.getLogger(WorkerProcess.class);

    private final WorkerProcessConfig config;
    private final int instanceId;
    private final ReusableLatch latch = new ReusableLatch(true);

    private final AtomicReference<AtLeastOnceProcessor<TestEvent>> processor = new AtomicReference<>();

    // Create the input, output, and state synchronizer streams (ignored if they already exist).
    public void init() {
        log.info("init: BEGIN");
        try (StreamManager streamManager = StreamManager.create(config.clientConfig)) {
            streamManager.createScope(config.scope);
            final StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(config.numSegments)).build();
            streamManager.createStream(config.scope, config.inputStreamName, streamConfig);
            streamManager.createStream(config.scope, config.outputStreamName, streamConfig);
            streamManager.createStream(
                    config.scope,
                    config.membershipSynchronizerStreamName,
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(config.scope, config.inputStreamName))
                .automaticCheckpointIntervalMillis(config.checkpointPeriodMs)
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(config.scope, config.clientConfig)) {
            // Create the Reader Group (ignored if it already exists)
            readerGroupManager.createReaderGroup(config.readerGroupName, readerGroupConfig);
        }
        log.info("init: END");
    }

    @Override
    protected void run() throws Exception {
        Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>() {}.getType());
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(config.scope, config.clientConfig)) {
            final ReaderGroup readerGroup = readerGroupManager.getReaderGroup(config.readerGroupName);
            // Create client factories.
            try (EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(config.scope, config.clientConfig);
                 SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(config.scope, config.clientConfig);
                 // Create a Pravega stream writer that we will send our processed output to.
                 EventStreamWriter<TestEvent> writer = eventStreamClientFactory.createEventWriter(
                         config.outputStreamName,
                         serializer,
                         EventWriterConfig.builder().build())) {

                final AtLeastOnceProcessor<TestEvent> proc = new AtLeastOnceProcessorInstrumented(
                        () -> ReaderGroupPruner.create(
                                readerGroup,
                                config.membershipSynchronizerStreamName,
                                Integer.toString(instanceId),
                                synchronizerClientFactory,
                                Executors.newScheduledThreadPool(1),
                                config.heartbeatIntervalMillis),
                        () -> eventStreamClientFactory.createReader(
                                Integer.toString(instanceId),
                                readerGroup.getGroupName(),
                                serializer,
                                ReaderConfig.builder().build()),
                        config.readTimeoutMillis,
                        instanceId,
                        writer,
                        latch);
                processor.set(proc);
                proc.startAsync();
                proc.awaitTerminated();
            }
        }
    }

    @Override
    protected void triggerShutdown() {
        log.info("triggerShutdown: BEGIN");
        final AtLeastOnceProcessor<TestEvent> proc = processor.getAndSet(null);
        if (proc != null) {
            proc.stopAsync();
        }
        latch.release();
        log.info("triggerShutdown: END");
    }

    public void pause() {
        latch.reset();
    }
}
