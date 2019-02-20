/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.https;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

public class HttpsReader {

    public static void main(String[] args) throws ReinitializationRequiredException {

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(Constants.CONTROLLER_URI)
                .trustStore(Constants.TRUSTSTORE_PATH)
                .validateHostName(false)
                .build();
        System.out.println("Done creating client config");

        ClientFactory clientFactory = null;
        ReaderGroupManager readerGroupManager = null;
        EventStreamReader<String> reader = null;
        try {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(Constants.SCOPE, Constants.STREAM_NAME))
                    .disableAutomaticCheckpoints()
                    .build();
            System.out.println("Done creating reader group config");

            readerGroupManager = ReaderGroupManager.withScope(Constants.SCOPE, clientConfig);
            readerGroupManager.createReaderGroup(Constants.READER_GROUP_NAME, readerGroupConfig);
            System.out.println("Done creating reader group with specified name and config");

            clientFactory = ClientFactory.withScope(Constants.SCOPE, clientConfig);
            System.out.println("Done creating a client factory with the specified scope and client config");

            reader = clientFactory.createReader("readerId", Constants.READER_GROUP_NAME,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());
            System.out.println("Done creating reader");

            String readMessage = reader.readNextEvent(1000).getEvent();
            System.out.println("Done reading message: [" + readMessage + "]");

        } finally {
            if (reader != null) reader.close();
            if (clientFactory != null) clientFactory.close();
            if (readerGroupManager != null) readerGroupManager.close();
        }
        System.out.println("HttpsReader is all done!");
    }
}
