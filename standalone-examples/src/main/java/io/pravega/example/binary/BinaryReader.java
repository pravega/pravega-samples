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
package io.pravega.example.binary;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Sequence;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

public class BinaryReader implements Runnable {
    private static final int READER_TIMEOUT_MS = 200000;

    private String scope;
    private String streamName;
    private URI controllerURI;
    private Consumer<ByteBuffer> onNext;
    private Consumer<Throwable> onError = (Throwable throwable) -> throwable.printStackTrace();

    public BinaryReader(String scope, String streamName, URI controllerURI, Consumer<ByteBuffer> onNext) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.onNext = onNext;
    }

    public void setOnError(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    public void run() {
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE)
                                                                     .build();

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig, Collections.singleton(streamName));
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                     readerGroup, new BinarySerializer(), ReaderConfig.builder().build())) {

            while (true) {
                try {
                    EventRead<ByteBuffer> event = reader.readNextEvent(READER_TIMEOUT_MS);
                    ByteBuffer eventData = event.getEvent();
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
