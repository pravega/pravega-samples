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
package io.pravega.example.consolerw;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * This class implements a simple console interface with the client for demonstration purposes. Specifically, this class
 * has two main objectives: i) Reads from a configured {@link Stream} until interrupted; ii) It allows developers to
 * have an easy first-time interaction with {@link StreamCut} API.
 */
@Slf4j
public class ConsoleReader implements Closeable {

    private final String scope;
    private final String streamName;
    private final URI controllerURI;

    private Map<Stream, StreamCut> streamCut;

    private ExecutorService executor;
    private BackgroundReader backgroundReader;

    private static final String[] MENU_TEXT = {
            "Enter one of the following commands at the command line prompt:",
            "",
            "Meanwhile, the program will read and display the events being written to the Stream.",
            "",
            "STREAMCUT_CREATE - create a StreamCut at the current point in which the reader is reading.",
            "STREAMCUT_READ_FROM - reads all the events in the Stream from the available StremCut up to the TAIL",
            "STREAMCUT_READ_UP_TO - reads all the events in the Stream from the HEAD up to the available StremCut",
            "HELP - print out a list of commands.",
            "QUIT - terminate the program."
    };

    public ConsoleReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.backgroundReader = new BackgroundReader(scope, streamName, controllerURI);
        executor = Executors.newSingleThreadExecutor();
    }

    /**
     * Use the console to accept commands from the command line and execute the commands against the stream.
     */
    public void run() throws IOException, InterruptedException {
        boolean done = false;

        outputHelp();

        // Start backgroundReader thread to display events being written from ConsoleWriter.
        executor.submit(backgroundReader);
        while(!done){
            String commandLine = readLine("%s >", scope + "/" + streamName).trim();
            if (! commandLine.equals("")) {
                done = processCommand(commandLine);
            }
        }

        // Closing threads and resources.
        backgroundReader.close();
        log.info("Waiting for backgroundReader thread to finish...");
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * Indirection to deal with Eclipse console bug #122429
     */
    private String readLine(String format, Object... args) throws IOException {
        if (System.console() != null) {
            return System.console().readLine(format, args);
        }
        System.out.println(String.format(format, args));
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }

    /**
     * The raw format of the command is COMMAND (not case sensitive).
     */
    private boolean processCommand(String rawString) {
        boolean ret = false;
        final Scanner sc = new Scanner(rawString);
        final String command = sc.next();
        List<String> parms;
        final String restOfLine;

        if (sc.hasNextLine()) {
            restOfLine = sc.nextLine();
            final String[] rawParms = restOfLine.split(",");
            parms = Arrays.asList(rawParms);
            parms.replaceAll(String::trim);
        } else {
            parms = new ArrayList<>();
        }

        switch(command.toUpperCase()) {
            case "STREAMCUT_CREATE":
                doCreateStreamCut();
                break;
            case "STREAMCUT_READ_FROM":
                doReadFromStreamCut();
                break;
            case "STREAMCUT_READ_UP_TO":
                doReadUpToStreamCut();
                break;
            case "HELP" :
                doHelp(parms);
                break;
            case "QUIT" :
                ret = true;
                output("Exiting...%n");
                break;
            default :
                output("Wrong option. Please, select a valid one...%n");
                break;
        }
        sc.close();
        return ret;
    }

    /**
     * This method gets the current {@link StreamCut} representing the last event read by the main loop for using it in
     * further calls that use {@link StreamCut}s for bounded processing.
     */
    private void doCreateStreamCut() {
        streamCut = backgroundReader.getLastStreamCut();
        output("New StreamCut: %s%n", streamCut.get(Stream.of(scope, streamName)).toString());
    }

    /**
     * This method uses {@link ReaderGroupConfig#builder()} to set a StreamCut as a start boundary on the events to
     * be read by readers. This means that a reader will only read events from the point represented by streamCut
     * variable until the tail of the {@link Stream}.
     */
    private void doReadFromStreamCut() {
        if (streamCut == null) {
            output("Please, create a StreamCut before trying to read from its position!%n");
            return;
        }
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName))
                                                    .startingStreamCuts(streamCut).build();
        readBasedOnStreamCuts(config);
    }

    /**
     * This method uses {@link ReaderGroupConfig#builder()} to set a StreamCut as a terminal boundary on the events to
     * be read by readers. This means that a reader will only read events from the head of the {@link Stream} up to the
     * point represented by streamCut variable.
     */
    private void doReadUpToStreamCut() {
        if (streamCut == null) {
            output("Please, create a StreamCut before trying to read up to its position!%n");
            return;
        }
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName))
                                                    .endingStreamCuts(streamCut).build();
        readBasedOnStreamCuts(config);
    }

    /**
     * This method shows a possible usage of {@link StreamCut}s related to bounded stream processing. The input
     * parameter has defined a {@link ReaderGroupConfig} with a {@link StreamCut} set to be either the initial or
     * terminal boundary for reading events. Once we create a {@link ReaderGroup} with this input configuration, then
     * all the readers belonging to this group will consume the events only within the defined boundaries.
     *
     * @param config Configuration for a {@link ReaderGroup} that will contain read boundaries in that {@link Stream}.
     */
    private void readBasedOnStreamCuts(ReaderGroupConfig config) {
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            // Create a reader group using the configuration with the defined StreamCut boundaries.
            readerGroupManager.createReaderGroup(readerGroup, config);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
             EventStreamReader<String> reader = clientFactory.createReader("streamcut-reader",
                     readerGroup, new JavaSerializer<>(), ReaderConfig.builder().build())) {

            // The reader(s) will only read and display events within the StreamCut boundaries defined.
            output("StreamCuts: Bounded processing example in stream %s/%s%n", scope, streamName);
            output("Starting boundary for readers: %s (UnboundedStreamCut represents the head of the stream)%n",
                    config.getStartingStreamCuts().get(Stream.of(scope, streamName)));
            output("Terminal boundary for readers: %s (UnboundedStreamCut represents the tail of the stream)%n",
                    config.getEndingStreamCuts().get(Stream.of(scope, streamName)));

            EventRead<String> event;
            do {
                event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    // TODO: Problem finding logback.xml in Pravega example applications (Issue #87).
                    output("[StreamCut read from/up to] Read event: %s%n", event.getEvent());
                    log.info("[StreamCut read from/up to] Read event: {}.", event.getEvent());
                }
            } while (event.getEvent() != null);
        } catch (ReinitializationRequiredException e) {
            // We do not expect this Exception from the reader in this situation, so we leave.
            log.error("Non-expected reader re-initialization.");
        } catch (IllegalArgumentException e) {
            log.warn("Nothing to read! Maybe your StreamCut is empty or at the head of the Stream and you are trying to" +
                    "read events up to it.");
        }
    }

    private void outputHelp () {
        Arrays.stream(MENU_TEXT).forEach(System.out::println);
        System.out.println(" ");
    }

    private void output(String format, Object... args){
        System.out.format("**** ");
        System.out.format(format, args);
    }

    private void doHelp(List<String> parms) {
        outputHelp();
        if (parms.size() > 0) {
            output("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            log.info("Exception parsing: {}.", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ConsoleReader", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();
        streamManager.createStream(scope, streamName, streamConfig);
        streamManager.close();

        ConsoleReader reader = new ConsoleReader(scope, streamName, controllerURI);
        reader.run();
        System.exit(0);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
}

/**
 * This class aims at continuously reading from the {@link Stream} and creating {@link StreamCut}s in a separate thread.
 */
@Slf4j
class BackgroundReader implements Closeable, Runnable {

    private static final int READER_TIMEOUT_MS = 1000;

    private final String scope;
    private final String streamName;
    private final URI controllerURI;
    private final String readerGroupName = UUID.randomUUID().toString().replace("-", "");
    private AtomicReference<Map<Stream, StreamCut>> lastStreamCut = new AtomicReference<>();

    private final AtomicBoolean end = new AtomicBoolean(false);
    private final ScheduledExecutorService executor;

    BackgroundReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        executor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * This method continuously performs two tasks: first, it reads events that are being written by console writer
     * or by any other process in that stream. Second, it creates a new StreamCut after every read event. The new
     * {@link StreamCut} represents the current tail of the {@link Stream} and it may be used to read events to or from
     * that position in the {@link Stream}.
     */
    public void run() {
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                     .stream(Stream.of(scope, streamName)).build();

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
             EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                     ClientConfig.builder().controllerURI(controllerURI).build())) {

            // Create the ReaderGroup to which readers will belong to.
            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
            @Cleanup
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);

            EventStreamReader<String> reader = clientFactory.createReader("backgroundReader", readerGroupName,
                    new JavaSerializer<>(), ReaderConfig.builder().build());
            EventRead<String> event;

            // Start main loop to continuously read and display events written to the scope/stream.
            log.info("Start reading events from {}/{}.", scope, streamName);
            do {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    // TODO: Problem finding logback.xml in Pravega example applications (Issue #87).
                    System.out.println("[BackgroundReader] Read event: " + event.getEvent());
                    log.info("[BackgroundReader] Read event: {}.", event.getEvent());
                }

                // Update the StreamCut after every event read, just in case the user wants to use it.
                if (!event.isCheckpoint()) {
                    readerGroup.initiateCheckpoint("myCheckpoint" + System.nanoTime(), executor)
                               .thenAccept(checkpoint -> lastStreamCut.set(checkpoint.asImpl().getPositions()));
                }
            } while (!end.get());
        } catch (ReinitializationRequiredException e) {
            // We do not expect this Exception from the reader in this situation, so we leave.
            log.error("Non-expected reader re-initialization.");
        }
    }

    Map<Stream, StreamCut> getLastStreamCut() {
        return lastStreamCut.get();
    }

    @Override
    public void close() {
        log.info("Closing background thread.");
        end.set(true);
        ExecutorServiceHelpers.shutdown(executor);
    }
}