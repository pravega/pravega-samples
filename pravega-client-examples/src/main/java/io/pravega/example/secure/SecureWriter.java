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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * This class demonstrates how to configure a Pravega writer application for:
 *
 *    a) SSL/TLS (HTTPS) communications with a Pravega cluster for data-in-transit encryption and
 *       server authentication.
 *    b) Passing credentials to a Pravega cluster for authentication and authorization of clients.
 *
 * This example can be driven interactively against a running Pravega cluster configured to communicate using SSL/TLS
 * and "auth" (authentication and authorization) turned on.
 */
public class SecureWriter {

    public static void main(String[] args) {

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

        // Everything below depicts the usual flow of writing events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

        System.out.println("Done creating client config");

        StreamManager streamManager = null;
        ClientFactory clientFactory = null;
        EventStreamWriter<String> writer = null;

        try {
            streamManager = StreamManager.create(clientConfig);
            System.out.println("Done creating a stream manager with the specified client config");

            streamManager.createScope(Constants.SCOPE);
            System.out.println("Done creating a scope with the specified name: [" + Constants.SCOPE + "]");

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(Constants.NO_OF_SEGMENTS))
                    .build();
            System.out.println("Done creating a stream configuration");

            streamManager.createStream(Constants.SCOPE, Constants.STREAM_NAME, streamConfig);
            System.out.println("Done creating a stream with the specified name: [" + Constants.STREAM_NAME
                    + "] and stream configuration");

            clientFactory = ClientFactory.withScope(Constants.SCOPE, clientConfig);
            System.out.println("Done creating a client factory with the specified scope and client config");

            writer = clientFactory.createEventWriter(
                    Constants.STREAM_NAME, new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            System.out.println("Done creating a writer");

            writer.writeEvent(Constants.MESSAGE);
            System.out.println("Done writing an event: [" + Constants.MESSAGE + "]");
        } finally {
            if (writer != null) writer.close();
            if (clientFactory != null) clientFactory.close();
            if (streamManager != null) streamManager.close();
        }
        System.err.println("All done with writing! Exiting...");
    }
}