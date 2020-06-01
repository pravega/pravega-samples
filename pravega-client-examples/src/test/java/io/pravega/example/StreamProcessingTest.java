package io.pravega.example;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.example.streamprocessing.AtLeastOnceProcessor;
import io.pravega.utils.SetupUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class StreamProcessingTest {
    static final Logger log = LoggerFactory.getLogger(StreamProcessingTest.class);

    protected static final AtomicReference<SetupUtils> SETUP_UTILS = new AtomicReference<>();

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.set(new SetupUtils());
        SETUP_UTILS.get().startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    public static void runWorker(final ClientConfig clientConfig,
                                 final String inputStreamName,
                                 final String readerGroup) throws Exception {

    }

    @Test
    public void basicTest() throws Exception {
        final String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        final String inputStreamName = "stream-" + UUID.randomUUID().toString();
        final String readerGroup = "rg" + UUID.randomUUID().toString().replace("-", "");
        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        final String readerId = "reader-" + UUID.randomUUID().toString();
        final ReaderConfig readerConfig = ReaderConfig.builder().build();
        final Serializer<String> serializer = new UTF8StringSerializer();
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, inputStreamName))
                .build();
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();

        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            streamManager.createScope(scope);

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(3))
                    .build();
            streamManager.createStream(scope, inputStreamName, streamConfig);

            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }
            try (final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 final EventStreamWriter<String> inputWriter = clientFactory.createEventWriter( inputStreamName, serializer, eventWriterConfig)) {

                inputWriter.writeEvent("CLAUDIO1");

                AtLeastOnceProcessor processor = new AtLeastOnceProcessor() {
                    @Override
                    public EventStreamReader<String> createReader() {
                        return clientFactory.createReader(readerId, readerGroup, serializer, readerConfig);
                    }

                    @Override
                    public void write(EventRead<String> eventRead) {
                    }
                };
                processor.call();
                // TODO: Wait for correct result and then terminate.
            }
        }
    }
}
