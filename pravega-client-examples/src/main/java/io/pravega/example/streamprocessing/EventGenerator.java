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

import com.google.gson.reflect.TypeToken;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app to write messages to a Pravega stream.
 */
public class EventGenerator {
    private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);

    private final AppConfiguration config;

    public static void main(String[] args) throws Exception {
        final EventGenerator app = new EventGenerator(new AppConfiguration(args));
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
            streamManager.updateStream(getConfig().getScope(), getConfig().getStream1Name(), streamConfig);
        }

        Random rand = new Random(42);
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(getConfig().getScope(), clientConfig);
             EventStreamWriter<SampleEvent> writer = clientFactory.createEventWriter(
                     getConfig().getStream1Name(),
                     new JSONSerializer<>(new TypeToken<SampleEvent>(){}.getType()),
                     EventWriterConfig.builder().build())) {
            long sequenceNumber = 0;
            long sum = 0;
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            for (;;) {
                sequenceNumber++;
                final SampleEvent event = new SampleEvent();
                event.sequenceNumber = sequenceNumber;
                event.routingKey = String.format("%3d", rand.nextInt(1000));
                event.intData = rand.nextInt(1000);
                sum += event.intData;
                event.sum = sum;
                event.timestamp = System.currentTimeMillis();
                event.timestampStr = dateFormat.format(new Date(event.timestamp));
                log.info("{}", event);
                final CompletableFuture<Void> writeFuture = writer.writeEvent(event.routingKey, event);
                final long ackedSequenceNumber = sequenceNumber;
                writeFuture.thenRun(() -> log.debug("Acknowledged: sequenceNumber={}", ackedSequenceNumber));
                Thread.sleep(1000);
            }
        }
    }

}
