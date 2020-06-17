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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app to write messages to a Pravega stream.
 */
public class EventGenerator {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EventGenerator.class);

    private final AppConfiguration config;

    public static void main(String[] args) throws Exception {
        EventGenerator app = new EventGenerator(new AppConfiguration(args));
        app.run();
    }

    public EventGenerator(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() throws Exception {
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(getConfig().getControllerURI()).build();
        try (StreamManager streamManager = StreamManager.create(getConfig().getControllerURI())) {
            streamManager.createScope(getConfig().getScope());
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            getConfig().getTargetRateEventsPerSec(),
                            getConfig().getScaleFactor(),
                            getConfig().getMinNumSegments()))
                    .build();
            streamManager.createStream(getConfig().getScope(), getConfig().getStream1Name(), streamConfig);
        }

        Random rand = new Random(42);
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(getConfig().getScope(), clientConfig);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     getConfig().getStream1Name(),
                     new UTF8StringSerializer(),
                     EventWriterConfig.builder().build())) {
            long eventCounter = 0;
            long sum = 0;
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            for (;;) {
                eventCounter++;
                long intData = rand.nextInt(100);
                String routingKey = String.format("%02d", intData);
                sum += intData;
                String generatedTimestampStr = dateFormat.format(new Date());
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
                final CompletableFuture<Void> writeFuture = writer.writeEvent(routingKey, message);
                Thread.sleep(1000);
            }
        }
    }

}
