package io.pravega.example.mqtt;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaHelper {

    private static Logger log = LoggerFactory.getLogger( PravegaHelper.class );

    public static EventStreamWriter<DataPacket> getStreamWriter(ApplicationArguments.PravegaArgs pravegaArgs) {
        log.info("Connecting to Pravega URI: {}, Scope: {}, Stream: {}",
                pravegaArgs.controllerUri, pravegaArgs.scope, pravegaArgs.stream);

        try (StreamManager streamManager = StreamManager.create(URI.create(pravegaArgs.controllerUri))) {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(pravegaArgs.targetRate, pravegaArgs.scaleFactor, pravegaArgs.minNumSegments))
                    .build();
            streamManager.createStream(pravegaArgs.scope, pravegaArgs.stream, streamConfig);
        }

        URI controllerURI = URI.create(pravegaArgs.controllerUri);
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(pravegaArgs.scope, clientConfig);
        EventStreamWriter<DataPacket> writer = clientFactory.createEventWriter(pravegaArgs.stream,
                new JavaSerializer<DataPacket>(),
                EventWriterConfig.builder().build());
        return writer;
    }
}
