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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
public class NonRecoverableSingleThreadedProcessor {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(NonRecoverableSingleThreadedProcessor.class);

    private static final int READER_TIMEOUT_MS = 2000;

    private final String scope;
    private final String inputStreamName;
    private final String outputStreamName;
    private final URI controllerURI;

    public NonRecoverableSingleThreadedProcessor(String scope, String inputStreamName, String outputStreamName, URI controllerURI) {
        this.scope = scope;
        this.inputStreamName = inputStreamName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
    }

    public static void main(String[] args) throws Exception {
        NonRecoverableSingleThreadedProcessor processor = new NonRecoverableSingleThreadedProcessor(
                Parameters.getScope(),
                Parameters.getStream1Name(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI());
        processor.run();
    }

    public void run() throws Exception {
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            Parameters.getTargetRateEventsPerSec(),
                            Parameters.getScaleFactor(),
                            Parameters.getMinNumSegments()))
                    .build();
            streamManager.createStream(scope, inputStreamName, streamConfig);
            streamManager.createStream(scope, outputStreamName, streamConfig);
        }

        // Create a reader group that begins at the earliest event.
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, inputStreamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamReader<String> reader = clientFactory.createReader(
                     "reader",
                     readerGroup,
                     new UTF8StringSerializer(),
                     ReaderConfig.builder().build());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     outputStreamName,
                     new UTF8StringSerializer(),
                     EventWriterConfig.builder().build())) {

            long eventCounter = 0;

            for (; ; ) {
                // Read input event.
                EventRead<String> eventRead = reader.readNextEvent(READER_TIMEOUT_MS);
                log.debug("readEvents: eventRead={}", eventRead);

                if (eventRead.getEvent() != null) {
                    eventCounter++;
                    log.debug("Read eventCounter={}, event={}", String.format("%06d", eventCounter), eventRead.getEvent());

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
                            String.format("%03d", 0),
                            generatedTimestampStr,
                            processedTimestampStr,
                            "");

                    // Write the output event.
                    log.info("eventCounter={}, event={}",
                            String.format("%06d", eventCounter),
                            message);
                    final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                }
            }
        }
    }
}
