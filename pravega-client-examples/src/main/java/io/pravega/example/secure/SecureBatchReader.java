/*
 * Copyright (c) 2020 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.secure;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.Futures;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static io.pravega.common.concurrent.ExecutorServiceHelpers.newScheduledThreadPool;

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
public class SecureBatchReader implements AutoCloseable {

    private final String scope;
    private final String stream;
    private final URI controllerURI;

    // TLS related config
    private final String truststorePath;
    private final boolean validateHostName;

    // Auth related config
    private final String username;
    private final String password;
    private ClientConfig clientConfig;

    private static final int THREAD_POOL_SIZE = 5;
    private final ScheduledExecutorService batchCountExecutor;

    public SecureBatchReader(String scope, String stream, URI controllerURI,
                             String truststorePath, boolean validateHostname,
                             String username, String password) {

        this.scope = scope;
        this.stream = stream;
        this.controllerURI = controllerURI;
        this.truststorePath = truststorePath;
        this.validateHostName = validateHostname;
        this.username = username;
        this.password = password;
        this.batchCountExecutor = newScheduledThreadPool(THREAD_POOL_SIZE, String.format("EventsCount"));
    }

    @Override
    public void close() throws Exception {
        this.batchCountExecutor.shutdownNow();
    }

    public void read() throws Exception {

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
        clientConfig = ClientConfig.builder()
                .controllerURI(this.controllerURI) // "tls://localhost:9090"

                // TLS-related client-side configuration
                .trustStore(this.truststorePath)
                .validateHostName(this.validateHostName)

                // Auth-related client-side configuration
                .credentials(new DefaultCredentials(this.password, this.username))
                .build();

        System.out.println("Done creating a client config.");

        // Everything below depicts the usual flow of reading events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

        try (BatchClientFactory batchClient = BatchClientFactory.withScope(scope, clientConfig)) {
            System.out.println("Done creating batchClient for " + scope + "/" + stream);
            ArrayList<SegmentRange> segments = new ArrayList<>();
            Iterator<SegmentRange> iterator =  batchClient.getSegments(Stream.of(scope, stream), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator();
            while(iterator.hasNext()) {
                  segments.add(iterator.next());
            }
            readFromSegments(batchClient, segments);
            System.out.println(String.format("Done reading %s segments.", segments.size()));
        }
        System.out.println("All done with reading! Exiting...");
    }

    public static void main(String[] args) throws Exception {
        Options options = getOptions();

        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
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

        SecureBatchReader reader = new SecureBatchReader(scope, stream, controllerURI,
                truststorePath, validateHostname,
                username, password);
        reader.read();
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "stream", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tls://host:port");
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

    private int readFromSegments(BatchClientFactory batchClient, List<SegmentRange> segments) throws Exception {
        List<Integer> batchEventCountList = new ArrayList<>();
        JavaSerializer<String> serializer = new JavaSerializer<>();
        int count = segments
                .stream()
                .mapToInt(segment -> {
                    SegmentIterator<String> segmentIterator = batchClient.readSegment(segment, serializer);
                    int numEvents = 0;
                    try {
                        String id = String.format("%s", Thread.currentThread().getId());
                        while (segmentIterator.hasNext()) {
                            String event = segmentIterator.next();
                            System.out.println("Done reading event by thread " + id + ": " + event);
                            numEvents++;
                        }
                    } finally {
                        segmentIterator.close();
                    }
                    return numEvents;
                }).sum();
        System.out.println(String.format("Done reading %s events", String.valueOf(count)));
        return count;
    }
}
