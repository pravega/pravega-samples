/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.datagenerator;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * Class that generates a fluctuating write workload. This is useful when demonstrating
 * auto-scaling use cases. In particular, event data written by this application contain
 * pathogen DNA samples from a public dataset.
 */
public class SinusoidalWorkloadGenerator {

    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    public final int scalingRate;

    public SinusoidalWorkloadGenerator(String scope, String streamName, URI controllerURI, int scalingRate) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.scalingRate = scalingRate;
    }

    public void run() throws InterruptedException {
        StreamManager streamManager = StreamManager.create(controllerURI);
        final AtomicLong sleepEvery = new AtomicLong(1);

        // Configure the Stream to scale every scalingRate events and to only keep the last hour of data.
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(this.scalingRate, 1, 1))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofHours(1)))
                .build();

        final boolean scopeIsNew = streamManager.createScope(scope);
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        System.out.println("Scope (created? " + scopeIsNew + ") and Stream (" + streamIsNew + ") ready for workload.");

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        @Cleanup
        EventStreamClientFactory factory = EventStreamClientFactory.withScope(scope, clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = factory.createEventWriter(streamName, new JavaSerializer<>(), writerConfig);

        // Sample genetic data.
        final String pathogenGeneticSequence = "@ERR4421719.1 NB551394:61:H7WCKBGXG:1:11101:24312:1057/1\n" +
                "GGTTTTTCACCAGCTGTTGGGTCAGGGTACTCGCCCCCTGCACCGTTCGGCCTGCCGTCAGGTTCGCCAGCACCGCGCGGCCGATGGAGTAG\n" +
                "+\n" +
                "AAAAAEEEEAEEEAEEEE6EEEEEEEEEEEEEEAEEEEEEE/EEEEEEEA/E<EE<EEAEEEAEE66E<AEA/EEEEEEEEEEEAEEEEEEA";
        Thread producerThread =
                new Thread(
                        () -> {
                            long i = 0;
                            while (true) {
                                writer.writeEvent(pathogenGeneticSequence);
                                i++;
                                if (i % sleepEvery.get() == 0) {
                                    System.out.println("Sleep interval " + sleepEvery.get() + " Pravega produced: " + i);
                                    try {
                                        Thread.sleep(10_000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                        break;
                                    }
                                }
                            }
                        });

        producerThread.start();
        System.out.println("Pravega producer started.");

        long median = 50_000L;
        long current;
        double in = -3;
        int i = 0;

        // This loop will change the sleepEvery variable used by the producerThread.
        // This will induce changes in the event rate written to Pravega.
        while (true) {
            current = median + (long) (median * Math.cos(in));
            sleepEvery.set(current);
            in += 0.04;
            System.out.println("At time " + (i++) + " Setting current " + current);
            Thread.sleep(30 * 1000L); // once per minute.
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("SinusoidalWorkloadGenerator entrypoint (args: " + String.join(",", args) + ")");
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
            System.out.println("cmd: " + cmd);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SinusoidalWorkloadGenerator", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope", Constants.DEFAULT_SCOPE);
        final String streamName = cmd.getOptionValue("stream", Constants.DEFAULT_STREAM_NAME);
        final String uriString = cmd.getOptionValue("uri", Constants.DEFAULT_CONTROLLER_URI);
        final URI controllerURI = URI.create(uriString);
        final int scalingRate = cmd.getOptionValue("scalingRate") == null ? Constants.DEFAULT_STREAM_SCALING_RATE :
                Integer.parseInt(cmd.getOptionValue("scalingRate"));

        System.out.println("Execution parameters: scope " + scope + ", stream " + streamName +
                ", uriString: " + uriString + ", scalingRate: " + scalingRate);
        SinusoidalWorkloadGenerator dw = new SinusoidalWorkloadGenerator(scope, streamName, controllerURI, scalingRate);
        dw.run();
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "stream", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        options.addOption("r", "scalingRate", true, "The rate in events/second to scale up a segment.");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
