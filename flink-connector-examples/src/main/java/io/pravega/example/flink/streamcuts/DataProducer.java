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
package io.pravega.example.flink.streamcuts;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DataProducer.class);

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
                                                                     .build();

        // Create a Pravega stream to write data (if it does not exist yet).
        streamManager.createStream(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM, streamConfiguration);

        // Create the client factory to instantiate writers and readers.
        try (ClientFactory clientFactory = ClientFactory.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI)) {

            // Create a writer to write events in the stream.
            EventStreamWriter<Double> writer = clientFactory.createEventWriter(Constants.PRODUCER_STREAM,
                    new JavaSerializer<>(), EventWriterConfig.builder().build());

            for (double i = 0; i < 10; i += 0.01) {
                writer.writeEvent(Math.sin(i)).join();
                LOG.warn("Writing event: {}.", Math.sin(i));
                Thread.sleep(100);
            }

            writer.close();
        }

        System.exit(0);
    }
}
