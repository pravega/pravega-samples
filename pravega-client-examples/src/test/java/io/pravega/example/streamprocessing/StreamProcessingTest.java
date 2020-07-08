package io.pravega.example.streamprocessing;

import com.google.common.collect.Iterators;
import com.google.gson.reflect.TypeToken;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
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
        SETUP_UTILS.set(new SetupUtils("tcp://localhost:9090"));
        SETUP_UTILS.get().startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    @Test
    public void noProcessorTest() throws Exception {
        final String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        // Create stream.
        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        final String inputStreamName = "stream-" + UUID.randomUUID().toString();
        SETUP_UTILS.get().createTestStream(inputStreamName, 6);
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Prepare writer that will write to the stream.
        final Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType());
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        @Cleanup
        final EventStreamWriter<TestEvent> writer = clientFactory.createEventWriter(inputStreamName, serializer, eventWriterConfig);

        // Prepare reader that will read from the stream.
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
        final EventStreamReader<TestEvent> reader = clientFactory.createReader(
                readerId,
                readerGroup,
                new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType()),
                readerConfig);
        EventStreamReaderIterator<TestEvent> readerIterator = new EventStreamReaderIterator<>(reader, 30000);

        // Create event generator instance.
        final TestEventGenerator generator = new TestEventGenerator(6);
        // Create event validator instance.
        final TestEventValidator validator = new TestEventValidator(generator);
        // Write historical events.
        Iterators.limit(generator, 13).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        // Read events from output stream. Return when complete or throw exception if out of order or timeout.
        validator.validate(readerIterator);
        Iterators.limit(generator, 3).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);
        Iterators.limit(generator, 15).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);
        log.info("SUCCESS");
    }

    @Test
    public void basicTest() throws Exception {
        final String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        final String inputStreamName = "input-stream-" + UUID.randomUUID().toString();
        final String outputStreamName = "output-stream-" + UUID.randomUUID().toString();
        final String membershipSynchronizerStreamName = "ms-" + UUID.randomUUID().toString();
        final String inputStreamReaderGroupName = "rg" + UUID.randomUUID().toString().replace("-", "");

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        final WorkerProcessConfig workerProcessConfig = WorkerProcessConfig.builder()
                .scope(scope)
                .clientConfig(clientConfig)
                .readerGroupName(inputStreamReaderGroupName)
                .inputStreamName(inputStreamName)
                .outputStreamName(outputStreamName)
                .membershipSynchronizerStreamName(membershipSynchronizerStreamName)
                .numSegments(6)
                .build();
        @Cleanup
        final WorkerProcessGroup workerProcessGroup = WorkerProcessGroup.builder().config(workerProcessConfig).build();

        // Start initial set of processors. This will also create the necessary streams.
        workerProcessGroup.start(0);

        // Prepare generator writer that will write to the stream read by the processor.
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        final Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType());
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        @Cleanup
        final EventStreamWriter<TestEvent> writer = clientFactory.createEventWriter(inputStreamName, serializer, eventWriterConfig);

        // Prepare validation reader that will read from the stream written by the processor.
        final String validationReaderGroupName = "rg" + UUID.randomUUID().toString().replace("-", "");
        final String validationReaderId = "reader-" + UUID.randomUUID().toString();
        final ReaderConfig validationReaderConfig = ReaderConfig.builder().build();
        final ReaderGroupConfig validationReaderGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, outputStreamName))
                .build();
        @Cleanup
        final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(validationReaderGroupName, validationReaderGroupConfig);
        @Cleanup
        final EventStreamReader<TestEvent> validationReader = clientFactory.createReader(
                validationReaderId,
                validationReaderGroupName,
                new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType()),
                validationReaderConfig);
        EventStreamReaderIterator<TestEvent> readerIterator = new EventStreamReaderIterator<>(validationReader, 30000);

        final TestEventGenerator generator = new TestEventGenerator(1);
        final TestEventValidator validator = new TestEventValidator(generator);

        // Write events to input stream.
        Iterators.limit(generator, 13).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        // Read events from output stream. Return when complete or throw exception if out of order or timeout.
        validator.validate(readerIterator);

        // Write and read additional events.
        Iterators.limit(generator, 3).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);
        Iterators.limit(generator, 15).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);

        log.info("getEventCountByInstanceId={}", validator.getEventCountByInstanceId());

        workerProcessGroup.start(3);
//        workerProcessGroup.stop(0);
        workerProcessGroup.pause(0);

        Iterators.limit(generator, 10).forEachRemaining(event -> writer.writeEvent(Integer.toString(event.key), event));
        validator.validate(readerIterator);

        log.info("getEventCountByInstanceId={}", validator.getEventCountByInstanceId());

        // Cleanup
        log.info("Cleanup");
        workerProcessGroup.close();
        validationReader.close();
        readerGroupManager.deleteReaderGroup(inputStreamReaderGroupName);
        readerGroupManager.deleteReaderGroup(validationReaderGroupName);
        streamManager.sealStream(scope, inputStreamName);
        streamManager.sealStream(scope, outputStreamName);
        streamManager.sealStream(scope, membershipSynchronizerStreamName);
        streamManager.deleteStream(scope, inputStreamName);
        streamManager.deleteStream(scope, outputStreamName);
        streamManager.deleteStream(scope, membershipSynchronizerStreamName);
        log.info("SUCCESS");
    }
}
