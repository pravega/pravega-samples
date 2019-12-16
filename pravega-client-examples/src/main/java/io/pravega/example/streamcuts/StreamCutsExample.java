/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.streamcuts;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamCutsExample implements Closeable {

    private static final int maxEventsPerDay = 3;
    private static final String eventSeparator = ":";
    private static final String streamSeparator = "-";

    private int numStreams;
    private int numEvents;
    private URI controllerURI;
    private String scope;
    private ScheduledExecutorService executor;
    private StreamManager streamManager;
    private List<String> myStreamNames = new ArrayList<>();
    private Map<String, SimpleEntry<Integer, Integer>> perDayEventIndex = new LinkedHashMap<>();

    public StreamCutsExample(int numStreams, int numEvents, String scope, URI controllerURI) {
        this.numStreams = numStreams;
        this.numEvents = numEvents;
        this.controllerURI = controllerURI;
        this.scope = scope;
        streamManager = StreamManager.create(controllerURI);
        executor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * A {@link StreamCut} is a collection of offsets, one for each open segment of a set of {@link Stream}s, which
     * indicates an event boundary. With a {@link StreamCut}, users can instruct readers to read from and/or up to a
     * particular event boundary (e.g., read events from 100 to 200, events created since Tuesday) on multiple
     * {@link Stream}s. To this end, Pravega allows us to create {@link StreamCut}s while readers are reading. In this
     * method, we read create two {@link StreamCut}s for a {@link Stream} according to the initial and final event
     * indexes passed by parameter.
     *
     * @param streamName Name of the {@link Stream} from which {@link StreamCut}s will be created.
     * @param iniEventIndex Index of the initial boundary for the {@link Stream} slice to process.
     * @param endEventIndex Index of the final boundary for the {@link Stream} slice to process.
     * @return Initial and final {@link Stream} boundaries represented as {@link StreamCut}s.
     */
    public List<StreamCut> createStreamCutsByIndexFor(String streamName, int iniEventIndex, int endEventIndex) {
        // Create the StreamCuts for the streams.
        final List<StreamCut> streamCuts = new ArrayList<>();
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());

        // Free resources after execution.
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(scope, controllerURI);
             EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                     ClientConfig.builder().controllerURI(controllerURI).build())) {

            // Create a reader group and a reader to read from the stream.
            final String readerGroupName = streamName + randomId;
            ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
            manager.createReaderGroup(readerGroupName, config);
            @Cleanup
            ReaderGroup readerGroup = manager.getReaderGroup(readerGroupName);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(randomId, readerGroup.getGroupName(),
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            // Read streams and create the StreamCuts during the read process.
            int eventIndex = 0;
            EventRead<String> event;
            do {
                // Here is where we create a StreamCut that points to the event indicated by the user.
                if (eventIndex == iniEventIndex || eventIndex == endEventIndex) {
                    reader.close();
                    streamCuts.add(readerGroup.getStreamCuts().get(Stream.of(scope, streamName)));
                    reader = clientFactory.createReader(randomId, readerGroup.getGroupName(),
                            new JavaSerializer<>(), ReaderConfig.builder().build());
                }

                event = reader.readNextEvent(1000);
                eventIndex++;
            } while (event.isCheckpoint() || event.getEvent() != null);

            // If there is only the initial StreamCut, this means that the final one is the tail of the stream.
            if (streamCuts.size() == 1) {
                streamCuts.add(StreamCut.UNBOUNDED);
            }
        } catch (ReinitializationRequiredException e) {
            // We do not expect this Exception from the reader in this situation, so we leave.
            log.error("Non-expected reader re-initialization.");
        }
        return streamCuts;
    }

    /**
     * This method is an example of bounded processing in Pravega with {@link StreamCut}s. {@link ReaderGroupConfig}
     * contains the information related to the {@link Stream}s to be read as well as the (optional) user-defined
     * boundaries in the form of {@link StreamCut}s that will limit the events to be read by reader processes. Note that
     * event readers (i.e., {@link EventStreamReader}) are agnostic to any notion of boundaries and they do not interact
     * with {@link StreamCut}s; they only consume events, which will be bounded within specific {@link Stream} slices as
     * configured in {@link ReaderGroupConfig}. The method basically creates a string representation of the events read
     * from {@link Stream}s within the bounds defined in the configuration parameter.
     *
     * @param config Configuration for the {@link ReaderGroup}, possibly containing {@link StreamCut} boundaries for
     *               limiting the number of events to read.
     * @return String representation of the events read by the reader.
     */
    public String printBoundedStreams(ReaderGroupConfig config) {
        StringBuilder result = new StringBuilder();
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(scope, controllerURI);
             EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                     ClientConfig.builder().controllerURI(controllerURI).build())) {
            final String readerGroupName = "RG" + randomId;
            manager.createReaderGroup(readerGroupName, config);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(randomId, readerGroupName,
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            // Write dummy events that identify each Stream.
            EventRead<String> event;
            do {
                event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    result = result.append(event.getEvent()).append('|');
                }

            } while (event.isCheckpoint() || event.getEvent() != null);

            result = result.append('\n');
        } catch (ReinitializationRequiredException e) {
            // We do not expect this Exception from the reader in this situation, so we leave.
            log.error("Non-expected reader re-initialization.");
        }
        return result.toString();
    }

    /**
     * A good use-case for {@link StreamCut}s is to allow efficient batch processing of data events within specific
     * boundaries (e.g., perform a mean on the temperature values in 1986). Instead of ingesting all the data and force
     * the reader to discard irrelevant events, {@link StreamCut}s help readers to only read the events that are
     * important for a particular task. In this sense, this method enables the Pravega {@link BatchClientFactory} to read from
     * various {@link Stream}s within the specific ranges passed as input, and the sum up all the values contained in
     * read events.
     *
     * @param streamCuts Map that defines the slices to read of a set of {@link Stream}s.
     * @return Sum of all the values of time series data belonging to {@link Stream}s and bounded by {@link StreamCut}s.
     */
    public int sumBoundedStreams(Map<Stream, List<StreamCut>> streamCuts) {
        int totalSumValuesInDay = 0;
        try (BatchClientFactory batchClient = BatchClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build())) {
            for (Stream myStream: streamCuts.keySet()) {

                // Get the cuts for this stream that will bound the number of events to read.
                final StreamCut startStreamCut = streamCuts.get(myStream).get(0);
                final StreamCut endStreamCut = streamCuts.get(myStream).get(1);

                // Then, we get the segment ranges according to the StreamCuts.
                StreamSegmentsIterator segments = batchClient.getSegments(myStream, startStreamCut, endStreamCut);
                List<SegmentRange> ranges = Lists.newArrayList(segments.getIterator());

                // We basically sum up all the values of events within the ranges.
                for (SegmentRange range: ranges) {
                    List<String> eventData = Lists.newArrayList(batchClient.readSegment(range, new JavaSerializer<>()));
                    totalSumValuesInDay += eventData.stream().map(s -> s.split(eventSeparator)[2]).mapToInt(Integer::valueOf).sum();
                }
            }
        }
        return totalSumValuesInDay;
    }

    // Region stream utils

    public void createAndPopulateStreamsWithNumbers() {
        Consumer<SimpleEntry> consumer = this::numericDataEvents;
        createAndPopulateStreams(consumer);
    }

    public void createAndPopulateStreamsWithDataSeries() {
        Consumer<SimpleEntry> consumer = this::dataSeriesEvents;
        createAndPopulateStreams(consumer);
    }

    public SimpleEntry<Integer, Integer> getStreamEventIndexesForDay(String streamName, int day) {
        return perDayEventIndex.get(getStreamDayKey(streamName, day));
    }

    /**
     * This method first creates the scope that will contain the streams to write and read events.
     */
    public void createAndPopulateStreams(Consumer<SimpleEntry> createDataEvents) {
        // Create the scope in first place, before creating the Streams.
        streamManager.createScope(scope);

        // Create Streams and write dummy events in them.
        for (char streamId = 'a'; streamId < 'a' + numStreams; streamId++) {
            String streamName = String.valueOf(streamId) + streamSeparator + System.nanoTime();
            myStreamNames.add(streamName);
            StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            streamManager.createStream(scope, streamName, streamConfig);

            // Note that we use the try-with-resources statement for those classes that should be closed after usage.
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                    ClientConfig.builder().controllerURI(controllerURI).build());
                 EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                         new JavaSerializer<>(), EventWriterConfig.builder().build())) {

                // Write data to the streams according to our preferences
                final SimpleEntry<EventStreamWriter<String>, String> writerAndStreamName = new SimpleEntry<>(writer, streamName);
                createDataEvents.accept(writerAndStreamName);
            }
        }
    }

    public void numericDataEvents(SimpleEntry<EventStreamWriter<String>, String> writerAndStreamName) {
        // Write dummy events that identify each Stream.
        StringBuilder sb = new StringBuilder();
        char streamBaseId = writerAndStreamName.getValue().charAt(0);
        for (int j = 0; j < numEvents; j++) {
            writerAndStreamName.getKey().writeEvent(sb.append(streamBaseId).append(j).toString()).join();
            sb.setLength(0);
        }
    }

    public void dataSeriesEvents(SimpleEntry<EventStreamWriter<String>, String> writerAndStreamName) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        int totalEventsSoFar = 0;
        char streamBaseId = writerAndStreamName.getValue().charAt(0);
        for (int i = 0; i < numEvents; i++) {
            final String daySuffix = eventSeparator + "day" + i;
            int eventsPerDay = random.nextInt(maxEventsPerDay);
            int lastDayEventIndex;

            // Write events specifying the day they belong to and the value in their content.
            for (lastDayEventIndex = 0; lastDayEventIndex < eventsPerDay; lastDayEventIndex++) {
                writerAndStreamName.getKey().writeEvent(sb.append(streamBaseId)
                                                          .append(daySuffix)
                                                          .append(eventSeparator)
                                                          .append(random.nextInt(20)).toString()).join();
                sb.setLength(0);
            }

            // Record the event indexes of events for day currentDayNumber
            if (lastDayEventIndex > 0) {
                perDayEventIndex.put(writerAndStreamName.getValue() + daySuffix,
                        new SimpleEntry<>(totalEventsSoFar, totalEventsSoFar + lastDayEventIndex));
                totalEventsSoFar += lastDayEventIndex;
            }
        }
    }

    /**
     * This method provides a print facility on the contents of all the {@link Stream}s.
     *
     * @return String containing the content of events for a specific {@link Stream}.
     */
    public String printStreams() {
        StringBuilder result = new StringBuilder();
        for (String streamName: myStreamNames) {
            ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
            result = result.append(printBoundedStreams(config));
        }

        return result.toString();
    }

    /**
     * We delete all the {@link Stream}s created every example execution.
     */
    public void deleteStreams() {
        // Delete the streams for next execution.
        for (String streamName: myStreamNames) {
            try {
                streamManager.sealStream(scope, streamName);
                Thread.sleep(500);
                streamManager.deleteStream(scope, streamName);
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Problem while sleeping current Thread in deleteStreams: ", e);
            }
        }
        myStreamNames.clear();
        perDayEventIndex.clear();
    }

    // End region stream utils

    /**
     * Close resources.
     */
    public void close() {
        streamManager.close();
        executor.shutdown();
    }

    public List<String> getMyStreamNames() {
        return myStreamNames;
    }

    private String getStreamDayKey (String streamName, int day) {
        return streamName + eventSeparator + "day" + day;
    }
}
