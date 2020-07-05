package io.pravega.example.streamprocessing;

import com.google.common.collect.Iterators;
import com.google.gson.reflect.TypeToken;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.utils.EventStreamReaderIterator;
import io.pravega.utils.SetupUtils;
import lombok.Cleanup;
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

    @Test
    public void basicTest() throws Exception {
        final String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        // Prepare writer that will write to the stream that will be the input to the processor.
        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        final String inputStreamName = "stream-" + UUID.randomUUID().toString();
        final Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType());
        SETUP_UTILS.get().createTestStream(inputStreamName, 6);
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        @Cleanup
        final EventStreamWriter<TestEvent> writer = clientFactory.createEventWriter(inputStreamName, serializer, eventWriterConfig);

        // Prepare reader that will read from the stream that will be the output from the processor.
        final String outputStreamName = inputStreamName;
        final String readerGroup = "rg" + UUID.randomUUID().toString().replace("-", "");
        final String readerId = "reader-" + UUID.randomUUID().toString();
        final ReaderConfig readerConfig = ReaderConfig.builder().build();
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(SETUP_UTILS.get().getStream(outputStreamName))
                .build();
        @Cleanup
        final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        @Cleanup
        EventStreamReader<TestEvent> reader = clientFactory.createReader(
                readerId,
                readerGroup,
                new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType()),
                readerConfig);
        EventStreamReaderIterator<TestEvent> readerIterator = new EventStreamReaderIterator<>(reader, 30000);

        // Create streams with specified segments.
        // Create event generator instance.
        TestEventGenerator generator = new TestEventGenerator(6);
        // Create event validator instance.
        TestEventValidator validator = new TestEventValidator(generator);
        // Create processor group instance.
        ProcessorGroup processorGroup;
        // Write 10 historical events.
        Iterators.limit(generator, 13).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        // Start processors.
//        processorGroup.start(new int[]{0, 1});
        // Read events from output stream. Return when complete or throw exception if out of order or timeout.
        validator.validate(readerIterator);
        // Kill some processors. Start some new ones.
//        processorGroup.gracefulStop(new int[]{0, 1});
        Iterators.limit(generator, 3).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);
        Iterators.limit(generator, 15).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);
        log.info("SUCCESS");
    }
}
