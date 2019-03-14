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

import java.io.FileOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * A simple example that demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 */
public class RecoverableMultithreadedProcessor {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RecoverableMultithreadedProcessor.class);

    public static class Master implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(Master.class);

        private static final String PRAVEGA_CHECKPOINT_FILE_NAME = "pravega-checkpoint";
        private static final String LATEST_CHECKPOINT_NAME_FILE_NAME = "latest";

        private final String scope;
        private final String inputStreamName;
        private final String outputStreamName;
        private final URI controllerURI;
        private final int numWorkers;
        private final String readerGroupName;
        private final ReaderGroup readerGroup;
        private final ScheduledExecutorService initiateCheckpointExecutor;
        private final ScheduledExecutorService performCheckpointExecutor;
        private final ExecutorService workerExecutor;
        private final ReaderGroupManager readerGroupManager;
        private final Path checkpointRootPath = Parameters.getCheckpointRootPath();
        private final Path latestCheckpointNamePath = checkpointRootPath.resolve(LATEST_CHECKPOINT_NAME_FILE_NAME);
        private final long checkpointPeriodMs = Parameters.getCheckpointPeriodMs();
        private final long checkpointTimeoutMs = Parameters.getCheckpointTimeoutMs();

        public Master(String scope, String inputStreamName, String outputStreamName, URI controllerURI, int numWorkers) throws Exception {
            this.scope = scope;
            this.inputStreamName = inputStreamName;
            this.outputStreamName = outputStreamName;
            this.controllerURI = controllerURI;
            this.numWorkers = numWorkers;

            // Create streams.
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
                streamManager.sealStream(scope, outputStreamName);
                streamManager.deleteStream(scope, outputStreamName);
                streamManager.createStream(scope, outputStreamName, streamConfig);
            }

            // Create a reader group manager. It must remain open to allow manual checkpoints to work.
            readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);

            ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder()
                    .disableAutomaticCheckpoints();

            // Attempt to load the last Pravega checkpoint.
            try {
                String checkpointName = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(latestCheckpointNamePath))).toString();
                Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
                Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
                log.info("Reading Pravega checkpoint from {}", checkpointPath);
                Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Files.readAllBytes(checkpointPath)));
                log.info("Starting from checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());
                builder = builder.startFromCheckpoint(checkpoint);
            } catch (Exception e) {
                log.warn("Unable to load checkpoint from {}. Starting from the earliest event.", checkpointRootPath, e);
                // This will create a reader group that starts from the earliest event.
                builder = builder.stream(Stream.of(scope, inputStreamName));
            }

            final ReaderGroupConfig readerGroupConfig = builder.build();
            readerGroupName = UUID.randomUUID().toString().replace("-", "");
            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
            readerGroup = readerGroupManager.getReaderGroup(readerGroupName);

            initiateCheckpointExecutor = Executors.newScheduledThreadPool(1);
            performCheckpointExecutor = Executors.newScheduledThreadPool(1);
            workerExecutor = Executors.newFixedThreadPool(numWorkers);

            // Schedule periodic task to initiate checkpoints.
            // If any execution of this task takes longer than its period, then subsequent executions may start late, but will not concurrently execute.
            initiateCheckpointExecutor.scheduleAtFixedRate(this::performCheckpoint, checkpointPeriodMs, checkpointPeriodMs, TimeUnit.MILLISECONDS);
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
        /**
         * Perform a checkpoint, wait for it to complete, and write the checkpoint to the state.
         */
        private void performCheckpoint() {
            final String checkpointName = UUID.randomUUID().toString();
            log.info("performCheckpoint: BEGIN: checkpointName={}", checkpointName);
            try {
                Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
                checkpointDirPath.toFile().mkdirs();

                final Set<String> onlineReaders = readerGroup.getOnlineReaders();
                log.info("performCheckpoint: onlineReaders ({})={}", onlineReaders.size(), onlineReaders);
                log.info("performCheckpoint: Calling initiateCheckpoint; checkpointName={}", checkpointName);
                CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, performCheckpointExecutor);
                log.debug("performCheckpoint: Got future.");
                Checkpoint checkpoint = checkpointFuture.get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
                log.info("performCheckpoint: Checkpoint completed; checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());

                Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
                log.info("Writing Pravega checkpoint to {}", checkpointPath);
                try (FileOutputStream fos = new FileOutputStream(checkpointPath.toFile())) {
                    fos.write(checkpoint.toBytes().array());
                    fos.flush();
                    fos.getFD().sync();
                }

                Path latestTmpCheckpointPath = checkpointRootPath.resolve(LATEST_CHECKPOINT_NAME_FILE_NAME + "tmp");
                try (FileOutputStream fos = new FileOutputStream(latestTmpCheckpointPath.toFile())) {
                    fos.write(checkpointName.getBytes(StandardCharsets.UTF_8));
                    fos.flush();
                    fos.getFD().sync();
                }
                Files.move(latestTmpCheckpointPath, latestCheckpointNamePath, StandardCopyOption.ATOMIC_MOVE);

                // TODO: Cleanup all other checkpoints.
            } catch (final Exception e) {
                log.warn("performCheckpoint: timed out waiting for checkpoint to complete", e);
                // Ignore error. We will retry when we are scheduled again.
            }
            log.info("performCheckpoint: END: checkpointName={}", checkpointName);
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
            readerId = "worker-" + this.workerIndex;
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

                    if (event.isCheckpoint()) {
                        log.info("Got checkpoint {}", event.getCheckpointName());
                    }
                    else if (event.getEvent() != null) {
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
