package io.pravega.example.streamcuts;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class StreamCutsConsole {

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

    public StreamCutsConsole(String scope, URI controllerURI) {
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
                do_simple_example();
                break;
            case "TIMESERIES":
                do_time_data_example();
                break;
            case "HELP" :
                doHelp(parms);
                break;
            case "QUIT" :
                ret = true;
                output("Exiting...%n");
                break;
            default :
                warn("Wrong option. Please, select a valid one...%n");
                break;
        }
        sc.close();
        return ret;
    }

    private void do_simple_example() throws IOException {
        final String prefix = "simple_example > ";
        final int exampleNumEvents = 10;
        final int exampleMaxStreams = 5;
        final char streamId = StreamCutsExample.streamBaseId;

        output("You have selected a simple example to see how Pravega StreamCuts work. Great choice!!%n");
        output("Now, we will ask you some questions to set up the example: %n%n");

        output("How many streams do you want to create (e.g., from 1 to 5):%n");
        int numStreams = askForIntInput(prefix, 0, exampleMaxStreams);

        // Set up and write data in streams.
        StreamCutsExample example = new StreamCutsExample(numStreams, exampleNumEvents, scope, controllerURI);

        example.createAndPopulateStreams();
        System.out.println(example.printStreams());

        output("Your Streams are ready :)%n");
        output("Now, to see how StreamCuts work, we are going to build them!%n");
        Map<Stream, StreamCut> startStreamCuts = new LinkedHashMap<>();
        Map<Stream, StreamCut> endStreamCuts = new LinkedHashMap<>();
        for (char i = streamId; i < streamId + numStreams; i++) {
            String streamName = String.valueOf(i);
            output("[Stream " + streamName + "] StreamCut start event number.%n");
            int iniEventIndex = askForIntInput(prefix, -1, exampleNumEvents);
            output("[Stream " + streamName + "] StreamCut end event number.%n");
            int endEventIndex = askForIntInput(prefix, iniEventIndex, exampleNumEvents);
            List<StreamCut> myStreamCuts = example.createStreamCutsFor(streamName, iniEventIndex, endEventIndex);
            startStreamCuts.put(Stream.of(scope, streamName), myStreamCuts.get(0));
            endStreamCuts.put(Stream.of(scope, streamName), myStreamCuts.get(1));
        }

        // Next, we demonstrate the capabilities of StreamCuts by enabling readers to perform bounded reads.
        ReaderGroupConfig.ReaderGroupConfigBuilder configBuilder = ReaderGroupConfig.builder();
        for (Stream myStream: startStreamCuts.keySet()) {
            configBuilder = configBuilder.stream(myStream);
        }

        // Here we enforce the boundaries for all the streams to be read, which enables bounded processing.
        ReaderGroupConfig config = configBuilder.startFromStreamCuts(startStreamCuts)
                                                .endingStreamCuts(endStreamCuts)
                                                .build();
        output("Now, look! We can read bounded slices of multiple Streams:%n%n");
        System.out.println(example.printBoundedStream(config));
        example.deleteStreams();
        example.close();

    }

    private void do_time_data_example() {
        // TODO: Create a second example with time series and BatchClient
        output("do_time_data_example");
    }

    private int askForIntInput(String prefix, int minVal, int maxVal) throws IOException {
        int result = Integer.MAX_VALUE;
        boolean firstAttempt = true;
        do {
            try {
                result = Integer.parseInt(readLine("%s", prefix).trim());
                if (firstAttempt) {
                    firstAttempt = false;
                } else {
                    warn("Please, numbers should be between (%s, %s] %n", minVal, maxVal);
                }
            } catch (NumberFormatException e) {
                warn("Please, introduce a correct number%n");
            }
        } while (result <= minVal || result > maxVal);
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

    private void warn(String format, Object... args){
        System.out.format("!!!! ");
        System.out.format(format, args);
    }

    private void doHelp(List<String> parms) {
        outputHelp();
        if (parms.size() > 0) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }

    public static void main(String[] args) throws IOException {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("StreamCuts", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        StreamCutsConsole console = new StreamCutsConsole(scope, controllerURI);
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
