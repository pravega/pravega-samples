package io.pravega.example.flink.watermark;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaWatermarkVerification {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PravegaWatermarkVerification.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        URI pravegaControllerURI = URI.create(params.get(Constants.CONTROLLER_ADDRESS_PARAM, Constants.CONTROLLER_ADDRESS));

        // StreamManager helps us to easily manage streams and copes.
        StreamManager streamManager = StreamManager.create(pravegaControllerURI);

        // A scope is a namespace that will be used to group streams (e.g., like dirs and files).
        streamManager.createScope(Constants.DEFAULT_SCOPE);

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(pravegaControllerURI)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // Create the client factory to instantiate writers and readers.
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(Constants.DEFAULT_SCOPE, pravegaConfig.getClientConfig())) {

            // Create a reader to read the final average event in the stream.
            EventStreamReader<SensorData> reader = clientFactory.createReader("avg-reader",
                    params.getRequired(Constants.INPUT_STREAM_PARAM),
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            EventRead<SensorData> eventRead = reader.readNextEvent(1000);
            SensorData finalAvg = eventRead.getEvent();

            System.out.println(finalAvg);
        }
    }
}
