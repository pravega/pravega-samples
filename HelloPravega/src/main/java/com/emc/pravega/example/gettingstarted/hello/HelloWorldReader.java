package com.emc.pravega.example.gettingstarted.hello;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;

/*
 * A simple example app that uses a Pravega Reader to read from a given scope and stream.
 */
public class HelloWorldReader {
    private static final String DEFAULT_SCOPE = "hello_scope";
    private static final String DEFAULT_STREAM_NAME = "hello_stream";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    
    private static final int READER_TIMEOUT_MS = 2000;

    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public HelloWorldReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run() {
        ClientFactory clientFactory = null;
        StreamManager streamManager = null;
        ReaderGroupManager readerGroupManager = null;
        EventStreamReader<String> reader = null;

        try {
            clientFactory = ClientFactory.withScope(scope, controllerURI);
            streamManager = StreamManager.withScope(scope, controllerURI);
            streamManager.createScope();

            StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scope).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

            streamManager.createStream(streamName, streamConfig);

            readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE)
                    .build();

            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig, Collections.singleton(streamName));

            reader = clientFactory.createReader("reader", readerGroup, new JavaSerializer<String>(),
                    ReaderConfig.builder().build());

            System.out.format("******** Reading all the events from %s/%s%n", scope, streamName);
            EventRead<String> event = null;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    System.out.format("******** Read event '%s'%n", event.getEvent());
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            } while (event.getEvent() != null);
            System.out.format("******** No more events from %s/%s%n", scope, streamName);
        } finally {
            if (reader != null) {
                System.out.println("******** closing reader ...");
                reader.close();
            }

            if (readerGroupManager != null) {
                System.out.println("******** closing reader group manager ...");
                readerGroupManager.close();
            }
            
            if (streamManager != null ) {
                System.out.println("******** closing stream manager ... ");
                streamManager.close();
            }

            if (clientFactory != null) {
                System.out.println("******** closing client factory ...");
                clientFactory.close();
            }
        }

    }

    public static void main(String[] args) {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldReader", options);
        }
        
        final String scope = cmd.getOptionValue("scope") == null ? DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        
        HelloWorldReader hwr = new HelloWorldReader(scope, streamName, controllerURI);
        hwr.run();
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
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
