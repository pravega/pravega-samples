/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flink.streamcuts.process;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.example.flink.streamcuts.Constants;
import java.net.URI;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended to produce suitable events for illustrating the Bookmarking service sample application. In
 * particular, the writer writes to the stream values corresponding to the result of Math.sin. This will help us to
 * show the ability to create stream cuts that are related to specific values of the sin function.
 */
public class DataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DataProducer.class);

    private static final int NUM_EVENTS = 10000;
    private static final double EVENT_VALUE_INCREMENT = 0.01; // Should be < 1
    private static final int WRITER_SLEEP_MS = 100;

    public static void main(String[] args) throws InterruptedException {
        // The writer will contact with the Pravega controller to get information about segments.
        URI pravegaControllerURI = URI.create("tcp://" + Constants.CONTROLLER_HOST + ":" + Constants.CONTROLLER_PORT);

        // StreamManager helps us to easily manage streams and copes.
        StreamManager streamManager = StreamManager.create(pravegaControllerURI);

        // A scope is a namespace that will be used to group streams (e.g., like dirs and files).
        streamManager.createScope(Constants.DEFAULT_SCOPE);

        // Here we configure the new stream (e.g., name, scope, scaling policy, retention policy).
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scope(Constants.DEFAULT_SCOPE)
                                                                     .streamName(Constants.PRODUCER_STREAM)
                                                                     .scalingPolicy(ScalingPolicy.fixed(Constants.PARALLELISM))
                                                                     .build();

        // Create a Pravega stream to write data (if it does not exist yet).
        streamManager.createStream(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM, streamConfiguration);

        // Create the client factory to instantiate writers and readers.
        try (ClientFactory clientFactory = ClientFactory.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI)) {

            // Create a writer to write events in the stream.
            EventStreamWriter<Tuple2<Integer, Double>> writer = clientFactory.createEventWriter(Constants.PRODUCER_STREAM,
                    new JavaSerializer<>(), EventWriterConfig.builder().build());

            for (double i = 0; i < NUM_EVENTS * EVENT_VALUE_INCREMENT; i += EVENT_VALUE_INCREMENT) {
                // Write an event for each sensor.
                for (int j = 0; j < Constants.PARALLELISM; j++) {
                    final double value = Math.sin(i + j); // Different values per sensor.
                    writer.writeEvent(String.valueOf(j), new Tuple2<>(j, value)).join();
                    LOG.warn("Writing event: {} (routing key {}).", new Tuple2<>(j, value), j);
                }

                Thread.sleep(WRITER_SLEEP_MS);
            }

            writer.close();
        }

        System.exit(0);
    }
}
