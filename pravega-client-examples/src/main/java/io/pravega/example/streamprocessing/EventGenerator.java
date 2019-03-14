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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app that to write messages to a Pravega stream.
 *
 * Use {@link ExactlyOnceMultithreadedProcessor} to read output events.
 */
public class EventGenerator {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EventGenerator.class);

    public final String scope;
    public final String outputStreamName;
    public final URI controllerURI;

    public EventGenerator(String scope, String outputStreamName, URI controllerURI) {
        this.scope = scope;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
    }

    public static void main(String[] args) throws Exception {
        EventGenerator processor = new EventGenerator(
                Parameters.getScope(),
                Parameters.getStream1Name(),
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
            streamManager.createStream(scope, outputStreamName, streamConfig);
        }

        Random rand = new Random(42);

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     outputStreamName,
                     new UTF8StringSerializer(),
                     EventWriterConfig.builder().build())) {
            long eventCounter = 0;
            long sum = 0;
            for (;;) {
                eventCounter++;
                String routingKey = String.format("rk%02d", eventCounter % 10);
                long intData = rand.nextInt(100);
                sum += intData;
                String generatedTimestampStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date());
                String message = String.join(",",
                        String.format("%06d", eventCounter),
                        routingKey,
                        String.format("%02d", intData),
                        String.format("%08d", sum),
                        generatedTimestampStr
                        );
                log.info("eventCounter={}, sum={}, event={}",
                        String.format("%06d", eventCounter),
                        String.format("%08d", sum),
                        message);
                final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                Thread.sleep(1000);
            }
        }
    }

}
