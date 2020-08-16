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
package io.pravega.example.noop;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;

import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;

public class SimpleReader<T> implements Runnable {
    private static final int READER_TIMEOUT_MS = 200000;

    private String scope;
    private String streamName;
    private URI controllerURI;
    private Serializer<T> serializer;
    private Consumer<T> onNext;
    private volatile boolean running;
    private Consumer<Throwable> onError = (Throwable throwable) -> throwable.printStackTrace();

    public SimpleReader(String scope, String streamName, URI controllerURI, Serializer<T> serializer, Consumer<T> onNext) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.serializer = serializer;
        this.onNext = onNext;
    }

    public void setOnError(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void run() {
        setRunning(true);
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
             EventStreamReader<T> reader = clientFactory.createReader("reader",
                     readerGroup, serializer, ReaderConfig.builder().build())) {

            while (isRunning()) {
                try {
                    EventRead<T> event = reader.readNextEvent(READER_TIMEOUT_MS);
                    T eventData = event.getEvent();
                    if (eventData != null) {
                        onNext.accept(event.getEvent());
                    }
                }
                catch (ReinitializationRequiredException e) {
                    onError.accept(e);
                }
            }
        }
    }
}
