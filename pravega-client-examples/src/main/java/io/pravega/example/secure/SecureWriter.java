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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;

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

    private final String scope;
    private final String stream;
    private final URI controllerURI;

    // TLS related config
    private final String truststorePath;
    private final boolean validateHostName;

    // Auth related config
    private final String username;
    private final String password;


    public SecureWriter(String scope, String stream, URI controllerURI,
                        String truststorePath, boolean validateHostname,
                        String username, String password) {

        this.scope = scope;
        this.stream = stream;
        this.controllerURI = controllerURI;
        this.truststorePath = truststorePath;
        this.validateHostName = validateHostname;
        this.username = username;
        this.password = password;
    }


    public void write(String routingKey, String message) {

        /*
         * Note about setting the client config for HTTPS:
         *    - The client config below is configured to use an optional truststore. The truststore is expected to be
         *      the certificate of the certification authority (CA) that was used to sign the server certificates.
         *      If this is null or empty, the default JVM trust store is used. In this demo, we use a provided
         *      "cert.pem" as the CA certificate, which is also provided on the server-side. If the cluster uses a
         *      different CA (which it should), use that CA's certificate as the truststore instead.
         *
         *    - Also, the client config below disables host name verification. If the cluster's server certificates
         *      have DNS names / IP addresses of the servers specified in them, you may turn this on. In a production
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
                .controllerURI(this.controllerURI) // "tls://localhost:9090"

                // TLS-related client-side configuration
                .trustStore(this.truststorePath)
                .validateHostName(this.validateHostName)

                // Auth-related client-side configuration
                .credentials(new DefaultCredentials(this.password, this.username))
                .build();

        // Everything below depicts the usual flow of writing events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

        System.out.println("Done creating client config.");

        StreamManager streamManager = null;
        EventStreamClientFactory clientFactory = null;
        EventStreamWriter<String> writer = null;

        try {
            streamManager = StreamManager.create(clientConfig);
            System.out.println("Done creating a stream manager.");

            streamManager.createScope(this.scope);
            System.out.println("Done creating a scope with the specified name: [" + this.scope + "].");

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(Constants.NO_OF_SEGMENTS))
                    .build();
            System.out.println("Done creating a stream configuration.");

            streamManager.createStream(this.scope, this.stream, streamConfig);
            System.out.println("Done creating a stream with the specified name: [" + this.stream
                    + "] and stream configuration.");

            clientFactory = EventStreamClientFactory.withScope(this.scope, clientConfig);
            System.out.println("Done creating a client factory with the specified scope and client config.");

            writer = clientFactory.createEventWriter(this.stream, new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            System.out.println("Done creating a writer.");

            writer.writeEvent(message);
            System.out.println("Done writing an event: [" + message + "].");
        } finally {
            if (writer != null) writer.close();
            if (clientFactory != null) clientFactory.close();
            if (streamManager != null) streamManager.close();
        }
        System.err.println("All done with writing! Exiting...");
    }

    public static void main(String[] args) {
        Options options = getOptions();

        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldReader", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String stream = cmd.getOptionValue("stream") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("stream");

        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        final String truststorePath =
                cmd.getOptionValue("truststore") == null ? Constants.DEFAULT_TRUSTSTORE_PATH : cmd.getOptionValue("truststore");
        final boolean validateHostname = cmd.getOptionValue("validatehost") == null ? false : true;

        final String username = cmd.getOptionValue("username") == null ? Constants.DEFAULT_USERNAME : cmd.getOptionValue("username");
        final String password = cmd.getOptionValue("password") == null ? Constants.DEFAULT_PASSWORD : cmd.getOptionValue("password");

        final String routingKey = cmd.getOptionValue("routingKey") == null ? Constants.DEFAULT_ROUTING_KEY : cmd.getOptionValue("routingKey");
        final String message = cmd.getOptionValue("message") == null ? Constants.DEFAULT_MESSAGE : cmd.getOptionValue("message");

        SecureWriter writer = new SecureWriter(scope, stream, controllerURI,
                truststorePath, validateHostname,
                username, password);
        writer.write(routingKey, message);
    }


    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "stream", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tls://host:port");
        options.addOption("r", "routingkey", true, "The routing key of the message to write.");
        options.addOption("m", "message", true, "The message to write.");
        options.addOption("t", "truststore", true,
                "The location of .pem truststore file in the file system to use by this application process.");
        options.addOption("v", "validatehost", false, "Whether to verify server's hostname.");
        options.addOption("a", "username", true,
                "The account username to use by the client for authenticating to the server.");
        options.addOption("p", "password", true,
                "The account password to use by the client for authenticating to the server.");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}