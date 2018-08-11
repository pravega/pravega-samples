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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
    static final int CHECKPOINT_INTERVAL = 5000;

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
        Stream inputStream = Utils.createStream(pravegaConfig, Constants.PRODUCER_STREAM);
        FlinkPravegaReader<Tuple2<Integer, Double>> reader = FlinkPravegaReader.<Tuple2<Integer, Double>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(inputStream)
                .withReaderGroupName(READER_GROUP_NAME)
                .withEventReadTimeout(Time.seconds(5))
                .withDeserializationSchema(new Tuple2DeserializationSchema())
                .build();

        // Create the Pravega sink to output the stream cuts representing slices to analyze.
        Stream outputStream = Utils.createStream(pravegaConfig, Constants.STREAMCUTS_STREAM);
        FlinkPravegaWriter<SensorStreamSlice> writer = FlinkPravegaWriter.<SensorStreamSlice>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStream)
                .withSerializationSchema(PravegaSerialization.serializationFor(SensorStreamSlice.class))
                .withEventRouter(new EventRouter())
                .build();

        // Initialize the Flink execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                                                                         .enableCheckpointing(CHECKPOINT_INTERVAL);

        // Bookmark those sections of the stream with values < 0 and write the output (StreamCuts).
        DataStreamSink<SensorStreamSlice> dataStreamSink = env.addSource(reader)
                                                              .setParallelism(Constants.PARALLELISM) // Num of parallel segments
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

    private transient ValueState<StreamCut> startStreamCut;
    private transient ValueState<StreamCut> lastStreamCutBeforeSlice;
    private transient ValueState<Tuple2<Integer, Double>> lastValue;

    public Bookmarker(URI pravegaControllerURI) {
        this.pravegaControllerURI = pravegaControllerURI;
    }

    @Override
    public void open(Configuration parameters) {
        // We will provide Bookmarker with a pointer to see the state of the reader group.
        startStreamCut = getRuntimeContext().getState(new ValueStateDescriptor<>("startStreamCut",
                TypeInformation.of(new TypeHint<StreamCut>() {})));
        lastStreamCutBeforeSlice = getRuntimeContext().getState(new ValueStateDescriptor<>("lastStreamCutBeforeSlice",
                TypeInformation.of(new TypeHint<StreamCut>() {})));
        lastValue = getRuntimeContext().getState(new ValueStateDescriptor<>("lastValue",
                TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {})));
    }

    /**
     * This function inspects the events written by a particular sensor (keyed stream by sensor id). When this function
     * finds that there are events < 0 it considers them as of interest for further processing. For this reason, it
     * gets a StreamCut related to the first observed event < 0 (start of stream slice). Then, if events become > 0
     * again, the function schedules a timer to get a future StreamCut (end of slice). That is, the SensorStreamSlice
     * object written to the collector will contain all the events < 0 for a certain sensor.
     *
     * @param value Input tuple from a sensor
     * @param ctx Context
     * @param out Collector
     * @throws IOException
     */
    @Override
    public void processElement(Tuple2<Integer, Double> value, Context ctx, Collector<SensorStreamSlice> out) throws IOException {
        if (lastValue.value() != null) return;
        // Infer the slice of the stream for which events are < 0.
        if (value.f1 < 0 && startStreamCut.value() == null) {
            startStreamCut.update(getReaderGroup().getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM)));
            LOG.warn("Start bookmarking a stream slice at: {} for sensor {}.", startStreamCut.value(), value.f0);
        } else if (value.f1 >= 0 && startStreamCut.value() != null && lastStreamCutBeforeSlice.value() == null) {
            lastStreamCutBeforeSlice.update(getReaderGroup().getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM)));
            lastValue.update(value);
            long sliceCreationTime = ctx.timerService().currentProcessingTime() + StreamBookmarker.CHECKPOINT_INTERVAL;
            LOG.warn("Found events > 0 for sensor {}, waiting for the next stream cut update to create the slice at {}.", value.f0, sliceCreationTime);
            ctx.timerService().registerProcessingTimeTimer(sliceCreationTime);
        }
    }

    /**
     * The frequency of StreamCut updates accessible from a Flink application is related to the frequency of checkpoints.
     * For this reason, to include all the events < 0, we need to create the slice ahead in time to wait for the next
     * checkpoint to complete. This ensures that the slice build will contain all the events of interest.
     *
     * @param timestamp timestamp
     * @param ctx OnTimerContext
     * @param out Collector
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorStreamSlice> out) throws Exception {
        // To get all the events < 0 in the output slice, we need to get the next streamcut w.r.t. the first one found when events were > 0.
        StreamCut nextStreamcut = getReaderGroup().getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM));
        if (lastStreamCutBeforeSlice.value().equals(nextStreamcut)) {
            LOG.error("The stream slice published may not contain all the events of interest");
        }

        SensorStreamSlice slice = new SensorStreamSlice(startStreamCut.value(), nextStreamcut, lastValue.value().f0);
        LOG.warn("Finish bookmarking a stream slice for sensor {} at: {}.", lastValue.value().f0, slice);
        out.collect(slice);
        startStreamCut.update(null);
        lastStreamCutBeforeSlice.update(null);
        lastValue.update(null);
    }

    private ReaderGroup getReaderGroup() {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI);
        return readerGroupManager.getReaderGroup(StreamBookmarker.READER_GROUP_NAME);
    }
}

class EventRouter implements PravegaEventRouter<SensorStreamSlice> {

    @Override
    public String getRoutingKey(SensorStreamSlice event) {
        // Ordering - events with the same routing key will always be read in the order they were written.
        return String.valueOf(event.getSensorId());
    }
}
