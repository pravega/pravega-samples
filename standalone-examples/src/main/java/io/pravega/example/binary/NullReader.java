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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

public class NullReader {
    private static final String DEFAULT_STREAM_ID = "examples/null";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";

    private AtomicLong eventsRead = new AtomicLong();
    private AtomicLong bytesRead = new AtomicLong();

    public void run(String scope, String streamName, URI controllerURI) throws InterruptedException {
        System.out.printf("Reading events from %s/%s\n", scope, streamName);

        BinaryReader binaryReader = new BinaryReader(scope, streamName, controllerURI, (ByteBuffer buffer) -> {
            bytesRead.addAndGet(buffer.remaining());
            eventsRead.incrementAndGet();
        });
        Thread thread = new Thread(binaryReader);
        thread.start();

        long startTime = System.currentTimeMillis();
        while (thread.isAlive()) {
            float deltaSeconds = (System.currentTimeMillis() - startTime) / 1000;
            long events = eventsRead.get();
            long bytes = bytesRead.get();
            System.out.printf("Events: %d read, %.1f/s -- Bytes: %d read, %.1f/s)\n",
                    events, events / deltaSeconds, bytes, bytes / deltaSeconds);
            Thread.sleep(30000);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Options options = getOptions();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            String[] streamId = StringUtils.split(cmd.getOptionValue("stream", DEFAULT_STREAM_ID), '/');
            if(streamId.length != 2) {
                throw new IllegalArgumentException("Stream spec must be in the form [scope]/[stream]");
            }

            final URI controllerURI = URI.create(cmd.getOptionValue("uri", DEFAULT_CONTROLLER_URI));
            new NullReader().run(streamId[0], streamId[1], controllerURI);
        }
        catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("NullReader", options);
            System.exit(1);
        }
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "stream", true, "The stream ID in the format [scope]/[stream].");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }
}
