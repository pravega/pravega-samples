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
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * See {@link ExactlyOnceMultithreadedProcessor}.
 */
public class ExactlyOnceMultithreadedProcessorWorker implements Callable<Void> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ExactlyOnceMultithreadedProcessorWorker.class);

    private static final int READER_TIMEOUT_MS = 2000;

    private final int workerIndex;
    private final String scope;
    private final String readerGroupName;
    private final boolean startFromCheckpoint;
    private final String startFromCheckpointName;
    private final String outputStreamName;
    private final URI controllerURI;
    private final String readerId;
    private final Path checkpointRootPath = Parameters.getCheckpointRootPath();

    public ExactlyOnceMultithreadedProcessorWorker(int workerIndex, String scope, String readerGroupName, String startFromCheckpointName, String outputStreamName, URI controllerURI) {
        this.workerIndex = workerIndex;
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.startFromCheckpointName = startFromCheckpointName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
        readerId = "worker-" + this.workerIndex;
        startFromCheckpoint = startFromCheckpointName != null;
    }

    public Void call() {
        Thread.currentThread().setName("worker-" + workerIndex);
        log.info("BEGIN");

        try {
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
                        Path transactionIdFilePath = checkpointDirPath.resolve(ExactlyOnceMultithreadedProcessor.CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX + this.workerIndex);

                        // Must ensure that txnId is persisted before committing transaction!
                        // Do not commit transaction here. Instead write TxnId to checkpoint directory. Master will read all TxnIds and commit transactions.

                        String transactionIds = "";
                        if (transaction != null) {
                            transaction.flush();
                            transactionIds = transaction.getTxnId().toString();
                            transaction = null;
                        }
                        Files.write(transactionIdFilePath, transactionIds.getBytes(StandardCharsets.UTF_8));
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

                        // Process the input event.
                        String processedTimestampStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date());

                        // Build the output event.
                        String message = String.join(",",
                                String.format("%06d", generatedEventCounter),
                                String.format("%06d", eventCounter),
                                routingKey,
                                String.format("%02d", intData),
                                String.format("%08d", generatedSum),
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
            // We don't handle incremental recovery of a single failed worker.
            // Stop the entire process (master and all workers).
            // When it is restarted, recovery of all workers will begin.
            System.exit(1);
        }
        return null;
    }
}
