/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flink.streamcuts.process;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.example.flink.Utils;
import io.pravega.example.flink.streamcuts.Constants;
import io.pravega.example.flink.streamcuts.SensorStreamSlice;
import io.pravega.example.flink.streamcuts.serialization.Tuple2DeserializationSchema;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended to read events from DataProducer and produce stream cuts that bookmark sections of the stream
 * with events exhibiting values of interest (e.g., event value < 0). Such stream cuts will we written to another stream
 * so that we can execute batch jobs on these stream slices.
 */
public class StreamBookmarker {

    private static final Logger LOG = LoggerFactory.getLogger(StreamBookmarker.class);

    static final String READER_GROUP_NAME = "bookmarkerReaderGroup" + System.currentTimeMillis();
    static final int CHECKPOINT_INTERVAL = 4000;

    public static void main(String[] args) throws Exception {
        // Initialize the parameter utility tool in order to retrieve input parameters.
        ParameterTool params = ParameterTool.fromArgs(args);

        // Clients will contact with the Pravega controller to get information about Streams.
        URI pravegaControllerURI = URI.create(params.get(Constants.CONTROLLER_ADDRESS_PARAM, Constants.CONTROLLER_ADDRESS));
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withControllerURI(pravegaControllerURI)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // Create the scope if it is not present.
        StreamManager streamManager = StreamManager.create(pravegaControllerURI);
        streamManager.createScope(Constants.DEFAULT_SCOPE);

        // Create the Pravega source to read from data produced by DataProducer.
        Stream sensorEvents = Utils.createStream(pravegaConfig, Constants.PRODUCER_STREAM);
        SourceFunction<Tuple2<Integer, Double>> reader = FlinkPravegaReader.<Tuple2<Integer, Double>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(sensorEvents)
                .withReaderGroupName(READER_GROUP_NAME)
                .withDeserializationSchema(new Tuple2DeserializationSchema())
                .build();

        // Create the Pravega sink to output the stream cuts representing slices to analyze.
        Stream streamCutsStream = Utils.createStream(pravegaConfig, Constants.STREAMCUTS_STREAM);
        SinkFunction<SensorStreamSlice> writer = FlinkPravegaWriter.<SensorStreamSlice>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(streamCutsStream)
                .withSerializationSchema(PravegaSerialization.serializationFor(SensorStreamSlice.class))
                .withEventRouter(new EventRouter())
                .build();

        // Initialize the Flink execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                                                                         .enableCheckpointing(CHECKPOINT_INTERVAL);

        // Bookmark those sections of the stream with values < 0 and write the output (StreamCuts).
        DataStreamSink<SensorStreamSlice> dataStreamSink = env.addSource(reader)
                                                              .setParallelism(Constants.PARALLELISM)
                                                              .keyBy(0)
                                                              .process(new Bookmarker(pravegaControllerURI))
                                                              .addSink(writer);

        // Execute within the Flink environment.
        env.execute("StreamBookmarker");
        LOG.info("Ending StreamBookmarker...");
    }
}

/**
 * This class processes the events read from the Pravega stream. Moreover, it also instantiates a reader group to check
 * the state of the Flink reader. The main task of this class is to output SensorStreamSlice objects that represent events
 * created by DataProducer whose value is < 0.
 */
class Bookmarker extends ProcessFunction<Tuple2<Integer, Double>, SensorStreamSlice>{

    private static final Logger LOG = LoggerFactory.getLogger(Bookmarker.class);
    private final URI pravegaControllerURI;
    private ReaderGroup readerGroup;

    private transient ValueState<Map<Integer, SensorStreamSlice>> pendingBookmarks;

    public Bookmarker(URI pravegaControllerURI) {
        this.pravegaControllerURI = pravegaControllerURI;
    }

    @Override
    public void open(Configuration parameters) {
        pendingBookmarks = getRuntimeContext().getState(new ValueStateDescriptor<>("pendingBookmarks",
                TypeInformation.of(new TypeHint<Map<Integer, SensorStreamSlice>>() {})));
    }

    /**
     * This function inspects the events written by a particular sensor (keyed stream by sensor id). When this function
     * finds that there are events < 0 it considers them as of interest for further processing. For this reason, it
     * gets a StreamCut before the first observed event < 0 (start of stream slice). Then, if events become > 0
     * again, the function ensures to get a StreamCut strictly beyond the point where the first event > 0 was observed
     * (end of slice). That is, the SensorStreamSlice object written to the collector will contain at least all the
     * events < 0 for a certain sensor.
     *
     * @param value Input tuple from a sensor
     * @param ctx Context
     * @param out Collector
     * @throws IOException
     */
    @Override
    public void processElement(Tuple2<Integer, Double> value, Context ctx, Collector<SensorStreamSlice> out) throws IOException {
        // Initialize task state for pending slices.
        if (pendingBookmarks.value() == null) {
            pendingBookmarks.update(new HashMap<>());
        }

        if (value.f1 < 0 && !pendingBookmarks.value().containsKey(value.f0)) {
            // Instantiate a SensorStreamSlice object for this sensor.
            SensorStreamSlice sensorStreamSlice = new SensorStreamSlice(value.f0);

            // Set the current ReaderGroup StreamCut as the beginning of the slice of events of interest.
            StreamCut startStreamCut = getReaderGroup().getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM));
            sensorStreamSlice.setStart(startStreamCut);
            Map<Integer, SensorStreamSlice> currentPendingBookmarks = pendingBookmarks.value();
            currentPendingBookmarks.put(value.f0, sensorStreamSlice);
            pendingBookmarks.update(currentPendingBookmarks);
            LOG.warn("Start bookmarking a stream slice at: {} for sensor {}.", startStreamCut, value.f0);
        } else if (value.f1 >= 0 && pendingBookmarks.value().containsKey(value.f0)) {
            SensorStreamSlice sensorStreamSlice = pendingBookmarks.value().get(value.f0);
            StreamCut currentStreamCut = getReaderGroup().getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM));

            // If this is the first event > 0, then we need to keep the current StreamCut to check for the next one.
            if (sensorStreamSlice.getEnd() == null) {
                LOG.warn("Initialize sensorStreamSlice end value to look for the next updated StreamCut: {} for sensor {}.", currentStreamCut, value.f0);
                sensorStreamSlice.setEnd(currentStreamCut);
            } else if (updatedStreamCutPositions(sensorStreamSlice.getEnd(), currentStreamCut)) {
                // Only when all the positions in the previous StreamCut are updated in the current one, we can ensure
                // that the slice contains all the evens of interest.
                sensorStreamSlice.setEnd(currentStreamCut);
                out.collect(sensorStreamSlice);
                LOG.warn("Found next end StreamCut for sensor {}: {}. The slice should contain all events < 0 for a specific sensor sine wave.", value.f0, currentStreamCut);
                Map<Integer, SensorStreamSlice> currentPendingBookmarks = pendingBookmarks.value();
                currentPendingBookmarks.remove(value.f0);
                pendingBookmarks.update(currentPendingBookmarks);
            }
        }
    }

    /**
     * This method returns a reference to the ReaderGroup being used by FlinkPravegaReader instance consuming events. As
     * ReaderGroups are identified by name, we can get this reference if we know the name of the targeted ReaderGroup.
     * This will allow us to get the StreamCuts from the reader positions working in the FlinkPravegaReader instance.
     *
     * @return Reference to the ReaderGroup used by FlinkPravegaReader.
     */
    private ReaderGroup getReaderGroup() {
        if (readerGroup == null) {
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI);
            readerGroup = readerGroupManager.getReaderGroup(StreamBookmarker.READER_GROUP_NAME);
        }

        return readerGroup;
    }

    /**
     * This method compares the positions of two StreamCuts. Concretely, this method verifies that all the positions
     * from the first input StreamCut are higher compared to the second input argument. This ensures that all the
     * reading positions of interest for the first StreamCut have been updated in the second one.
     *
     * @param streamCut StreamCut at a given point in time.
     * @param toCompare StreamCut to check if all its reading positions are higher compared to the first input argument.
     * @return Whether all the common positions between two StreamCuts are higher for the second one.
     */
    private boolean updatedStreamCutPositions(StreamCut streamCut, StreamCut toCompare) {
        Map<Segment, Long> streamCutPositions = streamCut.asImpl().getPositions();
        Map<Segment, Long> toComparePositions = toCompare.asImpl().getPositions();
        for (Segment s: streamCutPositions.keySet()) {
            if (toComparePositions.containsKey(s) && toComparePositions.get(s) <= streamCutPositions.get(s)) {
                return false;
            }
        }

        return true;
    }
}

class EventRouter implements PravegaEventRouter<SensorStreamSlice> {

    @Override
    public String getRoutingKey(SensorStreamSlice event) {
        // Ordering - events with the same routing key will always be read in the order they were written.
        return String.valueOf(event.getSensorId());
    }
}
