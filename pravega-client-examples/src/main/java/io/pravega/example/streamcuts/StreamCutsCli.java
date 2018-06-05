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
package io.pravega.example.streamcuts;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@Slf4j
public class StreamCutsCli {

    private static final int exampleMaxStreams = 5;
    private static final int exampleMaxEvents = 20;

    private final String scope;
    private final URI controllerURI;

    private static final String[] INITIAL_MENU = {
            "Enter one of the following commands at the command line prompt:",
            "",
            "SIMPLE - Simple yet illustrative example to show bounded processing with StreamCuts.",
            "TIMESERIES - This example gives a sense on the use of StreamCuts for batch processing.",
            "HELP - print out a list of commands.",
            "QUIT - terminate the program."
    };

    public StreamCutsCli(String scope, URI controllerURI) {
        this.scope = scope;
        this.controllerURI = controllerURI;
    }

    /**
     * Use the console to accept commands from the command line and execute the commands against the stream.
     */
    public void run() throws IOException {
        boolean done = false;

        outputHelp();

        while(!done){
            String commandLine = readLine("%s >", scope).trim();
            if (! commandLine.equals("")) {
                done = processCommand(commandLine);
            }
        }
    }

    /**
     * Indirection to deal with Eclipse console bug #122429
     */
    private String readLine(String format, Object... args) throws IOException {
        if (System.console() != null) {
            return System.console().readLine(format, args);
        }
        System.out.print(String.format(format, args));
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }

    /**
     * The raw format of the command is COMMAND (not case sensitive).
     */
    private boolean processCommand(String rawString) throws IOException {
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
            case "SIMPLE":
                doSimpleExample();
                break;
            case "TIMESERIES":
                doTimeSeriesExample();
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

    // Examples region

    private void doSimpleExample() throws IOException {
        final String prefix = "simple_example > ";

        output("You have selected a simple example to see how Pravega StreamCuts work. Great choice!!%n");
        output("Now, we will ask you some questions to set up the example: %n%n");

        output("How many streams do you want to create? (e.g., from 1 to %s):%n", exampleMaxStreams);
        final int numStreams = askForIntInput(prefix, 0, exampleMaxStreams);

        output("How many events per stream do you want to create? (e.g., from 1 to %s):%n", exampleMaxEvents);
        final int numEvents = askForIntInput(prefix, 0, exampleMaxEvents);

        // Set up and write data in streams.
        StreamCutsExample example = new StreamCutsExample(numStreams, numEvents, scope, controllerURI);
        example.createAndPopulateStreamsWithNumbers();
        System.out.println(example.printStreams());

        output("Your Streams are ready :)%n%n");
        output("Now, to see how StreamCuts work, we are going to build them!%n");

        // After setting up the streams and populating them, we can exercise StreamCuts multiple times.
        do {
            doBoundedPrinting(prefix, numStreams, numEvents, example);
            output("Do you want to repeat? (Y)%n");
        } while(readLine("%s", prefix).trim().equalsIgnoreCase("Y"));

        log.info("Sealing and deleting streams.");
        example.deleteStreams();
        example.close();
    }

    private void doBoundedPrinting(String prefix, int numStreams, int exampleNumEvents, StreamCutsExample example) throws IOException {
        Map<Stream, StreamCut> startStreamCuts = new LinkedHashMap<>();
        Map<Stream, StreamCut> endStreamCuts = new LinkedHashMap<>();

        for (String streamName: example.getMyStreamNames()) {
            output("[Stream %s]  StreamCut start event number.%n", streamName);
            int iniEventIndex = askForIntInput(prefix, 0, exampleNumEvents - 1);
            output("[Stream %s] StreamCut end event number.%n", streamName);
            int endEventIndex = askForIntInput(prefix, iniEventIndex + 1, exampleNumEvents);
            final List<StreamCut> myStreamCuts = example.createStreamCutsByIndexFor(streamName, iniEventIndex, endEventIndex);
            startStreamCuts.put(Stream.of(scope, streamName), myStreamCuts.get(0));
            endStreamCuts.put(Stream.of(scope, streamName), myStreamCuts.get(1));
        }

        // Here we enforce the boundaries for all the streams to be read, which enables bounded processing.
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(startStreamCuts)
                                                              .endingStreamCuts(endStreamCuts)
                                                              .build();
        output("Now, look! We can print bounded slices of multiple Streams:%n%n");
        output(example.printBoundedStreams(config));
    }

    private void doTimeSeriesExample() throws IOException {
        final String prefix = "timeseries_example > ";

        output("You have selected a timeseries example to see how Pravega StreamCuts work. Perfect!!%n");
        output("Now, we will ask you some questions to set up the example: %n%n");

        output("How many streams do you want to create? (e.g., from 1 to %s):%n", exampleMaxStreams);
        final int numStreams = askForIntInput(prefix, 1, exampleMaxStreams);

        output("How many days do you want to emulate in your data? (e.g., from 1 to %s):%n", exampleMaxEvents);
        final int exampleNumDays = askForIntInput(prefix, 1, exampleMaxEvents);

        // Set up and write data in streams.
        StreamCutsExample example = new StreamCutsExample(numStreams, exampleNumDays, scope, controllerURI);

        example.createAndPopulateStreamsWithDataSeries();
        System.out.println(example.printStreams());

        output("Your Streams are ready :)%n%n");
        output("We want to show the use of StreamCuts with BatchClient. To this end, we have created StreamCuts %n" +
               "for each stream that bound the events belonging to the same day.%n");
        output("The example consists of summing up all the values from events belonging to the same day (e.g., day1).%n%n");

        do {
            doBoundedSummingOfStreamValues(prefix, exampleNumDays, example);
            output("Do you want to repeat? (Y)%n");
        } while(readLine("%s", prefix).trim().equalsIgnoreCase("Y"));

        log.info("Sealing and deleting streams.");
        example.deleteStreams();
        example.close();
    }

    private void doBoundedSummingOfStreamValues(String prefix,int exampleNumDays, StreamCutsExample example) throws IOException {
        output("For which day number do you want to sum up values?.%n");
        int dayNumber = askForIntInput(prefix, 0, exampleNumDays);

        Map<Stream, List<StreamCut>> streamDayStreamCuts = new LinkedHashMap<>();
        for (String streamName: example.getMyStreamNames()) {
            final SimpleEntry<Integer, Integer> eventIndexesForDay = example.getStreamEventIndexesForDay(streamName, dayNumber);

            // Due to randomization, there could be streams with no events for a given day.
            if (eventIndexesForDay == null){
                continue;
            }
            output("[Stream %s] Indexes to bound day%s events: %s%n", streamName, dayNumber, eventIndexesForDay.toString());

            // Get the StreamCuts that define the event boundaries for the given day in this stream.
            final List<StreamCut> myStreamCuts = example.createStreamCutsByIndexFor(streamName, eventIndexesForDay.getKey(),
                    eventIndexesForDay.getValue());
            streamDayStreamCuts.put(Stream.of(scope, streamName), myStreamCuts);
        }

        // Next, we demonstrate the capabilities of StreamCuts by enabling readers to perform bounded reads.
        output("Now, look! We can sum up values from bounded slices of multiple Streams:%n%n");
        output("Result from summing all the values belonging to day%s is: %s!%n", dayNumber,
                example.sumBoundedStreams(streamDayStreamCuts));
    }

    // End examples region

    // Console utils region

    private int askForIntInput(String prefix, int minVal, int maxVal) throws IOException {
        int result = Integer.MAX_VALUE;
        boolean firstAttempt = true;
        do {
            try {
                result = Integer.parseInt(readLine("%s", prefix).trim());
                if (firstAttempt) {
                    firstAttempt = false;
                } else {
                    output("Please, numbers should be between [%s, %s] %n", minVal, maxVal);
                }
            } catch (NumberFormatException e) {
                output("Please, introduce a correct number%n");
            }
        } while (result < minVal || result > maxVal);
        return result;
    }

    private void outputHelp () {
        Arrays.stream(INITIAL_MENU).forEach(System.out::println);
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

    // End console utils region

    public static void main(String[] args) throws IOException {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            log.info("Exception parsing: {}.", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("StreamCuts", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        StreamCutsCli console = new StreamCutsCli(scope, controllerURI);
        console.run();
        System.exit(0);
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }
}
