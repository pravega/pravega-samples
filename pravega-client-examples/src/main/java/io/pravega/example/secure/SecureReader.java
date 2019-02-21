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
package io.pravega.example.secure;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * This class demonstrates how to configure a Pravega reader application for:
 *
 *    a) SSL/TLS (HTTPS) communications with a Pravega cluster for data-in-transit encryption and
 *       server authentication.
 *    b) Passing credentials to a Pravega cluster for authentication and authorization of clients.
 *
 * This example can be driven interactively against a running Pravega cluster configured to communicate using SSL/TLS
 * and "auth" (authentication and authorization) turned on.
 */
public class SecureReader {

    public static void main(String[] args) throws ReinitializationRequiredException {

        /**
         * Note about setting the client config for HTTPS:
         *    - The client config below is configured to use an optional truststore. The truststore is expected to be
         *      the certificate of the certification authority (CA) that was used to sign the server certificates.
         *      If this is null or empty, the default JVM trust store is used. In this demo, we use a provided
         *      "cert.pem" as the CA certificate, which is also provided on the server-side. If the cluster uses a
         *      different CA (which it should), use that CA's certificate as the truststore instead.
         *
         *    - Also, the client config below disables host name verification. If the cluster's server certificates
         *      have host names specified as the that of the server, you may turn this on. In a production
         *      deployment, it is recommended to keep this on.
         *
         * Note about setting the client config for auth:
         *    - The client config below is configured with an object of DefaultCredentials class. The user name
         *      and password arguments passed to the object represent the credentials used for authentication
         *      and authorization. The assumption we are making here is that the username is valid on the server,
         *      the password is correct and the username has all the permissions necessary for performing the
         *      subsequent operations.
         */
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(Constants.CONTROLLER_URI) // "tls://localhost:9090"
                .trustStore(Constants.TRUSTSTORE_PATH)   // SSL-related client-side configuration
                .validateHostName(false)                 // SSL-related client-side configuration
                .credentials(new DefaultCredentials("1111_aaaa", "admin")) // Auth-related client-side configuration
                .build();
        System.out.println("Done creating client config");

        // Everything below depicts the usual flow of reading events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

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

            String readMessage = reader.readNextEvent(2000).getEvent();
            System.out.println("Done reading message: [" + readMessage + "]");

        } finally {
            if (reader != null) reader.close();
            if (clientFactory != null) clientFactory.close();
            if (readerGroupManager != null) readerGroupManager.close();
        }
        System.err.println("All done with reading! Exiting...");
    }
}
