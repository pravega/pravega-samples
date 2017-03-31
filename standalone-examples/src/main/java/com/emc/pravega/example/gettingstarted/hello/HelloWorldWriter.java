package com.emc.pravega.example.gettingstarted.hello;

import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;

public class HelloWorldWriter {
    private static final String DEFAULT_SCOPE = "hello_scope";
    private static final String DEFAULT_STREAM_NAME = "hello_stream";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    private static final String DEFAULT_ROUTING_KEY = "hello_routingKey";
    private static final String DEFAULT_MESSAGE = "hello world";

    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public HelloWorldWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey, String message) {
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scope).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scope, streamName, streamConfig);

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                                                                                 new JavaSerializer<String>(),
                                                                                 EventWriterConfig.builder().build())) {
            
            System.out.format("******** Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                    message, routingKey, scope, streamName);
            writer.writeEvent(routingKey, message);
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
            formatter.printHelp("HelloWorldWriter", options);
        }

        final String scope = cmd.getOptionValue("scope") == null ? DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        
        HelloWorldWriter hww = new HelloWorldWriter(scope, streamName, controllerURI);
        
        final String routingKey = cmd.getOptionValue("routingKey") == null ? DEFAULT_ROUTING_KEY : cmd.getOptionValue("routingKey");
        final String message = cmd.getOptionValue("message") == null ? DEFAULT_MESSAGE : cmd.getOptionValue("message");
        hww.run(routingKey, message);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        options.addOption("r", "routingKey", true, "The routing key of the message to write.");
        options.addOption("m", "message", true, "The message to write.");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
