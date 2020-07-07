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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.admin.StreamInfo;
import io.pravega.common.concurrent.Futures;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.common.concurrent.Futures;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.shared.NameUtils;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.concurrent.Futures;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.TruncatedDataException;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.Random;




import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.UUID;
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
public class SecureBatchReader {

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

    private static Logger LOG = LoggerFactory.getLogger(SecureBatchReader.class);
    private static final int THREAD_POOL_SIZE = 5;
    private static ScheduledExecutorService streamCutLoggerService = newScheduledThreadPool(THREAD_POOL_SIZE, String.format("EventsCount"));
    private ScheduledExecutorService executor;


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
    }


    public void read() throws ReinitializationRequiredException {

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

        run();
        System.err.println("All done with reading! Exiting...");
    }

    public static void main(String[] args) throws ReinitializationRequiredException {
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

    public void run() {
        LOG.info("Starting segment logger");
        this.executor = newScheduledThreadPool(THREAD_POOL_SIZE, String.format("EventsLogger"));
        try {
            Futures.delayedTask(() -> {
                logEventCounts();
                this.executor.shutdownNow();
                return null;
            }, Duration.ofMinutes(0), executor).get();
        } catch (InterruptedException | ExecutionException e) {
           e.printStackTrace();
        }
        LOG.info("Started segment logger");
    }

    private void logEventCounts() {
        try (BatchClientFactory batchClient = BatchClientFactory.withScope(scope, clientConfig)) {
             ArrayList<SegmentRange> ranges = Lists.newArrayList(batchClient.getSegments(Stream.of(scope, stream), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
              readFromRanges(batchClient, ranges);
        } catch (Exception e) {
            LOG.error("Exception:", e);
        }
    }

    private int readFromRanges(BatchClientFactory batchClient, List<SegmentRange> ranges) {
        LOG.info("Number of ranges: {}", ranges.size());
        List<CompletableFuture<Integer>> eventCounts = new ArrayList<>();
        JavaSerializer<String> serializer = new JavaSerializer<String>();
        eventCounts = ranges
                .parallelStream()
                .map(range -> CompletableFuture.supplyAsync(() -> batchClient.readSegment(range, serializer))
                        .thenApplyAsync(segmentIterator -> {
                            int numEvents = 0;
                            try {
                                numEvents = Lists.newArrayList(segmentIterator).size();
                                String id = String.valueOf(Thread.currentThread().getId());
                                LOG.info("Thread " + id + " read " + String.valueOf(numEvents) + " events.");
                                while (segmentIterator.hasNext()) {
                                    String event = segmentIterator.next();
                                    System.out.println("Read event : " + event);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                segmentIterator.close();
                            }
                            return numEvents;
                        }))
                .collect(Collectors.toList());
        return eventCounts.stream().map(CompletableFuture::join).mapToInt(Integer::intValue).sum();
    }
}
