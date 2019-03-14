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

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A simple example that demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 *
 * This supports multiple worker threads.
 * Upon restart, it restarts from the last successful checkpoint and guarantees exactly-once semantics.
 *
 * Use {@link EventGenerator} to generate input events and {@link EventDebugSink}
 * to view the output events.
 */
public class ExactlyOnceMultithreadedProcessor {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ExactlyOnceMultithreadedProcessor.class);

    private static final String CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX = "pravega-transactions-worker-";

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
        private final boolean startFromCheckpoint;
        private final String startFromCheckpointName;
        private final ClientFactory clientFactory;
        private final EventStreamWriter<String> writer;

        public Master(String scope, String inputStreamName, String outputStreamName, URI controllerURI, int numWorkers) throws Exception {
            this.scope = scope;
            this.inputStreamName = inputStreamName;
            this.outputStreamName = outputStreamName;
            this.controllerURI = controllerURI;
            this.numWorkers = numWorkers;

            ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder()
                    .disableAutomaticCheckpoints();

            // Load the last checkpoint.
            startFromCheckpoint = latestCheckpointNamePath.toFile().exists();
            if (startFromCheckpoint) {
                // Read the name of the checkpoint from the file /tmp/checkpoint/latest.
                String checkpointName = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(latestCheckpointNamePath))).toString();
                Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
                Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
                log.info("Reading Pravega checkpoint from {}", checkpointPath);
                Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Files.readAllBytes(checkpointPath)));
                log.info("Starting from checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());
                builder = builder.startFromCheckpoint(checkpoint);
                startFromCheckpointName = checkpointName;
            } else {
                log.warn("Checkpoint file {} not found. Starting processing from the earliest event.", checkpointRootPath);

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
                    // Since we are starting processing from the beginning, delete and create a new output stream.
                    // TODO: Should we truncate stream instead of deleting?
                    try {
                        streamManager.sealStream(scope, outputStreamName);
                    } catch (Exception e) {
                        if (!(e.getCause() instanceof InvalidStreamException)) {
                            throw e;
                        }
                    }
                    // TODO: It would be nice if deleteStream did not require sealStream to be called.
                    streamManager.deleteStream(scope, outputStreamName);
                    streamManager.createStream(scope, outputStreamName, streamConfig);
                }

                // Create a reader group that starts from the earliest event.
                builder = builder.stream(Stream.of(scope, inputStreamName));
                startFromCheckpointName = null;
            }

            clientFactory = ClientFactory.withScope(scope, controllerURI);
            writer = clientFactory.createEventWriter(
                    outputStreamName,
                    new UTF8StringSerializer(),
                    EventWriterConfig.builder().build());

            // Create a reader group manager. It must remain open to allow manual checkpoints to work.
            readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);

            final ReaderGroupConfig readerGroupConfig = builder.build();
            readerGroupName = UUID.randomUUID().toString().replace("-", "");
            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
            readerGroup = readerGroupManager.getReaderGroup(readerGroupName);

            initiateCheckpointExecutor = Executors.newScheduledThreadPool(1);
            performCheckpointExecutor = Executors.newScheduledThreadPool(1);
            workerExecutor = Executors.newFixedThreadPool(numWorkers);
        }

        /**
         * Commit all transactions that are part of a checkpoint.
         *
         * @param checkpointName
         */
        private void commitTransactions(String checkpointName) {
            log.info("commitTransactions: BEGIN");

            // Read the contents of all pravega-transactions-worker-XX files.
            // These files contain the Pravega transaction IDs that must be committed now.
            Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
            List<UUID> txnIds = IntStream
                    .range(0, numWorkers)
                    .boxed()
                    .map(workerIndex -> checkpointDirPath.resolve(CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX + workerIndex))
                    .flatMap(path -> {
                        try {
                            return Files.readAllLines(path, StandardCharsets.UTF_8).stream();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(UUID::fromString)
                    .collect(Collectors.toList());

            log.info("commitTransactions: txnIds={}", txnIds);

            // Initiate commit of all transactions in the checkpoint.
            txnIds.parallelStream().forEach(txnId -> {
                try {
                    Transaction<String> transaction = writer.getTxn(txnId);
                    Transaction.Status status = transaction.checkStatus();
                    log.info("commitTransaction: transaction {} status is {}", transaction.getTxnId(), status);
                    if (status == Transaction.Status.OPEN) {
                        log.info("commitTransaction: committing {}", transaction.getTxnId());
                        transaction.commit();
                        // Note that commit may return before the transaction is committed.
                        // TODO: It would be nice for commit() to return a future when it becomes COMMITTED or ABORTED.
                    }
                } catch (TxnFailedException e) {
                    throw new RuntimeException(e);
                }
            });

            // Wait for commit of all transactions in the checkpoint.
            txnIds.parallelStream().forEach(txnId -> {
                try {
                    Transaction<String> transaction = writer.getTxn(txnId);
                    // TODO: Is there a better way to wait for COMMITTED besides polling?
                    for (; ; ) {
                        Transaction.Status status = transaction.checkStatus();
                        log.info("commitTransaction: transaction {} status is {}", transaction.getTxnId(), status);
                        if (status == Transaction.Status.COMMITTED) {
                            log.info("commitTransaction: committed {}", transaction.getTxnId());
                            break;
                        } else if (status == Transaction.Status.ABORTED) {
                            throw new RuntimeException(new TxnFailedException());
                        }
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            log.info("commitTransactions: END");
        }

        public void run() {
            // It is possible that the checkpoint was completely written but that some or all Pravega transactions
            // have not been committed. This will ensure that they are.
            if (startFromCheckpoint) {
                commitTransactions(startFromCheckpointName);
            }

            // Schedule periodic task to initiate checkpoints.
            // If any execution of this task takes longer than its period, then subsequent executions may start late, but will not concurrently execute.
            initiateCheckpointExecutor.scheduleAtFixedRate(this::performCheckpoint, checkpointPeriodMs, checkpointPeriodMs, TimeUnit.MILLISECONDS);

            // Start workers.
            IntStream.range(0, numWorkers).forEach(workerIndex -> {
                Worker worker = new Worker(workerIndex, scope, readerGroupName, startFromCheckpointName, outputStreamName, controllerURI);
                workerExecutor.submit(worker);
            });
            try {
                workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
            }
        }

        /**
         * Initiate a checkpoint, wait for it to complete, and write the checkpoint to the state.
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
                Checkpoint checkpoint = checkpointFuture.get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
                // At this point, all workers have received and processed the checkpoint.
                log.info("performCheckpoint: Checkpoint completed; checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());

                Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
                log.info("Writing Pravega checkpoint to {}", checkpointPath);
                try (FileOutputStream fos = new FileOutputStream(checkpointPath.toFile())) {
                    fos.write(checkpoint.toBytes().array());
                    fos.flush();
                    fos.getFD().sync();
                }

                // Create "latest" file that indicates the latest checkpoint name.
                // This file must be updated atomically.
                Path latestTmpCheckpointPath = checkpointRootPath.resolve(LATEST_CHECKPOINT_NAME_FILE_NAME + ".tmp");
                try (FileOutputStream fos = new FileOutputStream(latestTmpCheckpointPath.toFile())) {
                    fos.write(checkpointName.getBytes(StandardCharsets.UTF_8));
                    fos.flush();
                    fos.getFD().sync();
                }
                Files.move(latestTmpCheckpointPath, latestCheckpointNamePath, StandardCopyOption.ATOMIC_MOVE);

                // Read list of TxnIds from checkpoint directory written by all workers and commit all transactions.
                commitTransactions(checkpointName);

                cleanCheckpointDirectory(checkpointDirPath);
            } catch (final Exception e) {
                log.warn("performCheckpoint: timed out waiting for checkpoint to complete", e);
                // Ignore error. We will retry when we are scheduled again.
            }
            log.info("performCheckpoint: END: checkpointName={}", checkpointName);
        }

        /**
         * Delete everything in the checkpoint root path (/tmp/checkpoint) except the "latest" file and
         * the latest checkpoint directory.
         *
         * @param keepCheckpointDirPath The latest checkpoint directory which will not be deleted.
         */
        private void cleanCheckpointDirectory(Path keepCheckpointDirPath) {
            try {
                Files.walkFileTree(checkpointRootPath,
                        new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes basicFileAttributes) throws IOException {
                                if (dir.equals(keepCheckpointDirPath)) {
                                    return FileVisitResult.SKIP_SUBTREE;
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                                if (!dir.equals(checkpointRootPath)) {
                                    Files.delete(dir);
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (!file.equals(latestCheckpointNamePath)) {
                                    Files.delete(file);
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } catch (IOException e) {
                log.warn("cleanCheckpointDirectory", e);
            }
        }
    }

    public static class Worker implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(Worker.class);

        private static final String STATE_FILE_NAME_PREFIX = "state-worker-";
        private static final int READER_TIMEOUT_MS = 2000;

        private static class State implements Serializable {
            private static final long serialVersionUID = -275148988691911596L;

            long sum;

            public State() {
                this.sum = 0;
            }

            @Override
            public String toString() {
                return "State{" +
                        "sum=" + sum +
                        '}';
            }
        }

        private final int workerIndex;
        private final String scope;
        private final String readerGroupName;
        private final boolean startFromCheckpoint;
        private final String startFromCheckpointName;
        private final String outputStreamName;
        private final URI controllerURI;
        private final String readerId;
        private final Path checkpointRootPath = Parameters.getCheckpointRootPath();

        private State state;

        public Worker(int workerIndex, String scope, String readerGroupName, String startFromCheckpointName, String outputStreamName, URI controllerURI) {
            this.workerIndex = workerIndex;
            this.scope = scope;
            this.readerGroupName = readerGroupName;
            this.startFromCheckpointName = startFromCheckpointName;
            this.outputStreamName = outputStreamName;
            this.controllerURI = controllerURI;
            readerId = "worker-" + this.workerIndex;
            startFromCheckpoint = startFromCheckpointName != null;
        }

        public void run() {
            Thread.currentThread().setName("worker-" + workerIndex);
            log.info("BEGIN");

            try {
                // Load state from checkpoint.
                if (startFromCheckpoint) {
                    Path checkpointDirPath = checkpointRootPath.resolve(startFromCheckpointName);
                    Path statePath = checkpointDirPath.resolve(STATE_FILE_NAME_PREFIX + this.workerIndex);
                    log.info("statePath={}", statePath.toString());
                    try (FileInputStream fis = new FileInputStream(statePath.toString());
                         ObjectInputStream ois = new ObjectInputStream(fis)) {
                        state = (State) ois.readObject();
                    }
                    log.info("Loaded state {} from {}", state, statePath);
                } else {
                    log.info("Initializing with new state");
                    state = new State();
                }

                try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                     EventStreamReader<String> reader = clientFactory.createReader(
                             readerId,
                             readerGroupName,
                             new UTF8StringSerializer(),
                             ReaderConfig.builder().build());
                     EventStreamWriter<String> writer = clientFactory.createEventWriter(
                             outputStreamName,
                             new UTF8StringSerializer(),
                             EventWriterConfig.builder()
                                     .transactionTimeoutTime(Parameters.getTransactionTimeoutMs())
                                     .build())) {

                    Transaction<String> transaction = null;
                    long eventCounter = 0;

                    for (; ; ) {
                        // Read input event.
                        EventRead<String> eventRead = reader.readNextEvent(READER_TIMEOUT_MS);
                        log.debug("readEvents: eventRead={}", eventRead);

                        if (eventRead.isCheckpoint()) {
                            // Note that next call readNextEvent will indicate to Pravega that we are done with the checkpoint.
                            String checkpointName = eventRead.getCheckpointName();
                            log.info("Got checkpoint {}", eventRead.getCheckpointName());
                            Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
                            Path transactionIdFilePath = checkpointDirPath.resolve(CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX + this.workerIndex);

                            // Must ensure that txnId is persisted to latest state before committing transaction!
                            // Do not commit transaction here. Instead write TxnId to checkpoint directory. Master will read all TxnIds and commit transactions.

                            String transactionIds = "";
                            if (transaction != null) {
                                transaction.flush();
                                transactionIds = transaction.getTxnId().toString();
                                transaction = null;
                            }
                            Files.write(transactionIdFilePath, transactionIds.getBytes(StandardCharsets.UTF_8));

                            // Write state to checkpoint directory
                            Path statePath = checkpointDirPath.resolve(STATE_FILE_NAME_PREFIX + this.workerIndex);
                            log.info("statePath={}", statePath.toString());
                            try (FileOutputStream fos = new FileOutputStream(statePath.toString());
                                 ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                                oos.writeObject(state);
                                oos.flush();
                                fos.getFD().sync();
                            }

                        } else if (eventRead.getEvent() != null) {
                            eventCounter++;
                            log.debug("Read eventCounter={}, event={}", String.format("%06d", eventCounter), eventRead.getEvent());

                            if (transaction == null) {
                                transaction = writer.beginTxn();
                            }

                            // Parse input event.
                            String[] cols = eventRead.getEvent().split(",");
                            long generatedEventCounter = Long.parseLong(cols[0]);
                            String routingKey = cols[1];
                            long intData = Long.parseLong(cols[2]);
                            long generatedSum = Long.parseLong(cols[3]);
                            String generatedTimestampStr = cols[4];

                            // Process the input event and update the state.
                            state.sum += intData;
                            String processedTimestampStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date());

                            // Build the output event.
                            String message = String.join(",",
                                    String.format("%06d", generatedEventCounter),
                                    String.format("%06d", eventCounter),
                                    routingKey,
                                    String.format("%02d", intData),
                                    String.format("%08d", generatedSum),
                                    String.format("%08d", state.sum),
                                    String.format("%03d", workerIndex),
                                    generatedTimestampStr,
                                    processedTimestampStr,
                                    transaction.getTxnId().toString());

                            // Write the output event.
                            log.info("eventCounter={}, event={}",
                                    String.format("%06d", eventCounter),
                                    message);
                            transaction.writeEvent(routingKey, message);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Fatal Error", e);
                System.exit(1);
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
