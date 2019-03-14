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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * A simple example that demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 *
 * This supports multiple worker threads.
 * Upon restart, it reprocesses the entire input stream and recreates the output stream.
 *
 * Use {@link EventGenerator} to generate input events and {@link EventDebugSink}
 * to view the output events.
 *
 * See {@link ExactlyOnceMultithreadedProcessor} for an improved version.
 */
public class NonRecoverableMultithreadedProcessor {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(NonRecoverableMultithreadedProcessor.class);

    public static class Master implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(Master.class);

        private final String scope;
        private final String inputStreamName;
        private final String outputStreamName;
        private final URI controllerURI;
        private final int numWorkers;
        private final String readerGroupName;
        private final ReaderGroup readerGroup;
        private final ExecutorService workerExecutor;

        public Master(String scope, String inputStreamName, String outputStreamName, URI controllerURI, int numWorkers) {
            this.scope = scope;
            this.inputStreamName = inputStreamName;
            this.outputStreamName = outputStreamName;
            this.controllerURI = controllerURI;
            this.numWorkers = numWorkers;

            try (StreamManager streamManager = StreamManager.create(controllerURI)) {
                streamManager.createScope(scope);
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.byEventRate(
                                Parameters.getTargetRateEventsPerSec(),
                                Parameters.getScaleFactor(),
                                Parameters.getMinNumSegments()))
                        .build();
                streamManager.createStream(scope, inputStreamName, streamConfig);
                // Since we start reading the input stream from the earliest event, we must delete the output stream.
                try {
                    streamManager.sealStream(scope, outputStreamName);
                } catch (Exception e) {
                    if (!(e.getCause() instanceof InvalidStreamException)) {
                        throw e;
                    }
                }
                streamManager.deleteStream(scope, outputStreamName);
                streamManager.createStream(scope, outputStreamName, streamConfig);
            }

            // Create a reader group that begins at the earliest event.
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, inputStreamName))
                    .build();
            readerGroupName = UUID.randomUUID().toString().replace("-", "");
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
                readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
            }

            workerExecutor = Executors.newFixedThreadPool(numWorkers);
        }

        public void run() {
            IntStream.range(0, numWorkers).forEach(workerIndex -> {
                Worker worker = new Worker(workerIndex, scope, readerGroupName, outputStreamName, controllerURI);
                workerExecutor.submit(worker);
            });
            try {
                workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
            }
        }
    }

    public static class Worker implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(Worker.class);

        private static class State {
            long sum;

            public State(long sum) {
                this.sum = sum;
            }
        }

        private static final int readerTimeoutMs = 2000;

        private final int workerIndex;
        private final String scope;
        private final String readerGroupName;
        private final String outputStreamName;
        private final URI controllerURI;
        private final String readerId;

        private State state;

        public Worker(int workerIndex, String scope, String readerGroupName, String outputStreamName, URI controllerURI) {
            this.workerIndex = workerIndex;
            this.scope = scope;
            this.readerGroupName = readerGroupName;
            this.outputStreamName = outputStreamName;
            this.controllerURI = controllerURI;
            readerId = "reader" + this.workerIndex;
        }

        public void run() {
            Thread.currentThread().setName("worker-" + workerIndex);
            log.info("BEGIN");
            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<String> reader = clientFactory.createReader(
                         readerId,
                         readerGroupName,
                         new UTF8StringSerializer(),
                         ReaderConfig.builder().build());
                 EventStreamWriter<String> writer = clientFactory.createEventWriter(
                         outputStreamName,
                         new UTF8StringSerializer(),
                         EventWriterConfig.builder().build())) {

                // Initialize state.
                state = new State(0);

                EventRead<String> event;
                for (int i = 0; ; i++) {
                    // Read input event.
                    try {
                        event = reader.readNextEvent(readerTimeoutMs);
                    } catch (ReinitializationRequiredException e) {
                        // There are certain circumstances where the reader needs to be reinitialized
                        log.error("Read error", e);
                        throw new RuntimeException(e);
                    }

                    if (event.getEvent() != null) {
                        log.info("Read event '{}'", event.getEvent());

                        // Parse input event.
                        String[] cols = event.getEvent().split(",");
                        String routingKey = cols[0];
                        long intData = Long.parseLong(cols[1]);
                        long generatedIndex = Long.parseLong(cols[2]);
                        String generatedTimestampStr = cols[3];

                        // Process the input event and update the state.
                        state.sum += intData;
                        String processedTimestampStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date());

                        // Build the output event.
                        String message = String.join(",",
                                routingKey,
                                String.format("%02d", intData),
                                String.format("%08d", generatedIndex),
                                String.format("%08d", i),
                                generatedTimestampStr,
                                processedTimestampStr,
                                String.format("%d", state.sum));

                        // Write the output event.
                        log.info("Writing message '{}' with routing key '{}' to stream {}/{}",
                                message, routingKey, scope, outputStreamName);
                        final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Master master = new Master(
                Parameters.getScope(),
                Parameters.getStream1Name(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI(),
                Parameters.getNumWorkers());
        master.run();
    }
}
