package io.pravega.example.streamcuts;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import lombok.Cleanup;

public class StreamCutsExample implements Closeable {

    public static final char streamBaseId = 'a';

    private int numStreams;
    private int numEvents;
    private URI controllerURI;
    private String scope;
    private ScheduledExecutorService executor;
    private StreamManager streamManager;

    public StreamCutsExample(int numStreams, int numEvents, String scope, URI controllerURI) {
        this.numStreams = numStreams;
        this.numEvents = numEvents;
        this.controllerURI = controllerURI;
        this.scope = scope;
        streamManager = StreamManager.create(controllerURI);
        executor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * This method first creates the scope that will contain the streams to write and read events. Then, we write
     * numEvents at each stream.
     */
    public void createAndPopulateStreams() {
        // Create the scope in first place, before creating the Streams.
        streamManager.createScope(scope);

        // Create Streams and write dummy events in them.
        for (char id = streamBaseId; id < streamBaseId + numStreams; id++) {
            String streamName = String.valueOf(id);
            StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            System.out.println("Stream " + id + " has been created? " + !streamManager.createStream(scope, streamName, streamConfig));

            // Note that we use the try-with-resources statement for those classes that should be closed after usage.
            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                         new JavaSerializer<>(), EventWriterConfig.builder().build())) {

                // Write dummy events that identify each Stream.
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < numEvents; j++) {
                    writer.writeEvent(sb.append(streamName).append(j).toString()).join();
                    sb.setLength(0);
                }
            }
        }
    }

    /**
     * A {@link StreamCut} is an offset that indicates an event boundary. With a {@link StreamCut}, users can instruct
     * readers to read from or up to a particular boundary (e.g., read events from 100 to 200, events created since
     * Tuesday). To this end, Pravega allows us to create {@link StreamCut}s while readers are processing a stream
     * (e.g., via a {@link Checkpoint}). In this method, we read a {@link Stream} and create two {@link StreamCut}s,
     * according to the initial and final event indexes needed to read a slice of a {@link Stream}.
     *
     * @param streamName Name of the {@link Stream} from which {@link StreamCut}s will be created.
     * @param iniEventIndex Index of the initial boundary for the {@link Stream} slice to process.
     * @param endEventIndex Index of the final boundary for the {@link Stream} slice to process.
     * @return Initial and final {@link Stream} boundaries represented as {@link StreamCut}s.
     */
    public List<StreamCut> createStreamCutsFor(String streamName, int iniEventIndex, int endEventIndex) {
        // Create the StreamCuts for the streams.
        final List<StreamCut> streamCuts = new ArrayList<>();
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());

        // Free resources after execution.
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(scope, controllerURI);
             ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
            final String readerGroupName = streamName + randomId;
            ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
            manager.createReaderGroup(readerGroupName, config);
            @Cleanup
            ReaderGroup readerGroup = manager.getReaderGroup(readerGroupName);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(randomId, readerGroupName,
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            // Read streams and create the StreamCuts during the read process.
            Checkpoint checkpoint;
            for (int j = 0; j < numEvents; j++) {

                // Here is where we create a StreamCut that points to the event indicated by the user.
                if (j == iniEventIndex || j == endEventIndex) {
                    reader.close();
                    checkpoint = readerGroup.initiateCheckpoint(randomId + j, executor).join();
                    streamCuts.add(checkpoint.asImpl().getPositions().values().iterator().next());
                    reader = clientFactory.createReader(randomId, readerGroupName,
                            new JavaSerializer<>(), ReaderConfig.builder().build());
                }
                try {
                    reader.readNextEvent(1000);
                } catch (ReinitializationRequiredException e) {
                    e.printStackTrace();
                }
            }

            // This StreamCut represents the tail of the Stream
            if (endEventIndex == numEvents) {
                streamCuts.add(StreamCut.UNBOUNDED);
            }
        }
        return streamCuts;
    }

    /**
     * This method is an example of bounded processing in Pravega with {@link StreamCut}s. {@link ReaderGroupConfig}
     * contains the information related to the {@link Stream}s to be read as well as the (optional) user-defined
     * boundaries in the form of {@link StreamCut}s that will limit the events to be read by reader processes. Note that
     * event readers (i.e., {@link EventStreamReader}) are agnostic to any notion of boundaries and they do not interact
     * with {@link StreamCut}s; they only consume events, which will be bounded within specific {@link Stream} slices as
     * configured in {@link ReaderGroupConfig}.
     *
     * @param config Configuration for the {@link ReaderGroup}, possibly containing {@link StreamCut} boundaries for
     *               limiting the number of events to read.
     * @return String representation of the events read by the reader.
     */
    public String printBoundedStream(ReaderGroupConfig config) {
        StringBuilder result = new StringBuilder();
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(scope, controllerURI);
             ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
            final String readerGroupName = "RG" + randomId;
            manager.createReaderGroup(readerGroupName, config);

            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(randomId, readerGroupName,
                    new JavaSerializer<>(), ReaderConfig.builder().build());
            // Write dummy events that identify each Stream.
            EventRead<String> event = null;
            do {
                try {
                    event = reader.readNextEvent(1000);
                    if (event.getEvent() != null) {
                        result = result.append(event.getEvent()).append('|');
                    }
                } catch (ReinitializationRequiredException e) {
                    e.printStackTrace();
                }
            } while (event.isCheckpoint() || event.getEvent() != null);
            result = result.append('\n');
        }
        return result.toString();
    }

    /**
     * This method provides a print facility on the contents of all the {@link Stream}s.
     *
     * @return String containing the content of events for a specific {@link Stream}.
     */
    public String printStreams() {
        StringBuilder result = new StringBuilder();
        for (char id = streamBaseId; id < streamBaseId + numStreams; id++) {
            final String streamName = String.valueOf(id);
            ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
            result = result.append(printBoundedStream(config));
        }
        return result.toString();
    }

    /**
     * We delete all the {@link Stream}s created every example execution.
     */
    public void deleteStreams() {
        // Delete the streams for next execution.
        for (char id = streamBaseId; id < streamBaseId + numStreams; id++) {
            // FIXME: The second time the example is executed, it fails as it cannot work on already deleted streams
            // (we need to restart Pravega to make it work)
            try {
                streamManager.sealStream(scope, String.valueOf(id));
                Thread.sleep(500);
                streamManager.deleteStream(scope, String.valueOf(id));
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Close resources.
     */
    public void close() {
        streamManager.close();
        executor.shutdown();
    }
}
