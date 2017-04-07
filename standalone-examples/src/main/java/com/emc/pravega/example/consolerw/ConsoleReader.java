package com.emc.pravega.example.consolerw;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.StreamManager;
import com.emc.pravega.example.gettingstarted.hello.HelloWorldReader;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;

/**
 * Reads from a Stream until interrupted.
 */
public class ConsoleReader {
    
    private static final int READER_TIMEOUT_MS = 200000;
    
    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public ConsoleReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }
    
    public void run() {
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE)
                .build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scope).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scope, streamName, streamConfig);

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig, Collections.singleton(streamName));
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamReader<String> reader = clientFactory.createReader("reader",
                                                                           readerGroup,
                                                                           new JavaSerializer<String>(),
                                                                           ReaderConfig.builder().build())) {
            System.out.format("******** Reading events from %s/%s%n", scope, streamName);
            EventRead<String> event = null;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if(event != null) {
                        System.out.format("'%s'%n", event.getEvent());
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            }while(true);
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
            formatter.printHelp("ConsoleReader", options);
        }
        
        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        
        ConsoleReader reader = new ConsoleReader(scope, streamName, controllerURI);
        reader.run();
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
