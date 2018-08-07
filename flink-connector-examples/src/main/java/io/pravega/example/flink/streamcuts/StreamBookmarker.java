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
package io.pravega.example.flink.streamcuts;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.example.flink.Utils;
import java.net.URI;
import org.apache.flink.api.common.time.Time;
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

    // The writer will contact with the Pravega controller to get information about segments.
    static final URI pravegaControllerURI = URI.create("tcp://" + Constants.CONTROLLER_HOST + ":" + Constants.CONTROLLER_PORT);

    static final String READER_GROUP_NAME = "bookmarkerReaderGroup" + System.currentTimeMillis();

    private static final Logger LOG = LoggerFactory.getLogger(StreamBookmarker.class);

    public static void main(String[] args) throws Exception {
        // Initialize the parameter utility tool in order to retrieve input parameters.
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // Create the Pravega source to read from data produced by DataProducer.
        Stream inputStream = Utils.createStream(pravegaConfig, Constants.PRODUCER_STREAM);
        FlinkPravegaReader<Double> reader = FlinkPravegaReader.<Double>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(inputStream)
                .withReaderGroupName(READER_GROUP_NAME)
                .withReaderGroupRefreshTime(Time.milliseconds(100))
                .withDeserializationSchema(PravegaSerialization.deserializationFor(Double.class))
                .build();

        // Create the Pravega sink to output the stream cuts representing slices to analyze.
        Stream outputStream = Utils.createStream(pravegaConfig, Constants.STREAMCUTS_STREAM);
        FlinkPravegaWriter<StreamSlice> writer = FlinkPravegaWriter.<StreamSlice>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStream)
                .withSerializationSchema(PravegaSerialization.serializationFor(StreamSlice.class))
                .withEventRouter(new EventRouter())
                .build();

        // Initialize the Flink execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Bookmark those sections of the stream with values < 0 and write the output (StreamCuts).
        DataStreamSink<StreamSlice> dataStreamSink = env.addSource(reader)
                                                        .process(new Bookmarker())
                                                        .setParallelism(1)
                                                        .addSink(writer);

        // Execute within the Flink environment.
        env.execute("StreamBookmarker");
        LOG.info("Ending StreamBookmarker...");
    }
}

/**
 * This class processes the events read from the Pravega stream. Moreover, it also instantiates a reader group to check
 * the state of the Flink reader. The main task of this class is to output StreamSlice objects that represent events
 * created by DataProducer whose value is < 0.
 */
class Bookmarker extends ProcessFunction<Double, StreamSlice>{

    private static final Logger LOG = LoggerFactory.getLogger(Bookmarker.class);

    // We need the reader group of the Flink job to get the stream cut info.
    private ReaderGroupManager readerGroupManager;

    private StreamCut startStreamCut;

    @Override
    public void open(Configuration parameters) {
        // We will provide Bookmarker with a pointer to see the state of the reader group.
        this.readerGroupManager = ReaderGroupManager.withScope(Constants.DEFAULT_SCOPE, StreamBookmarker.pravegaControllerURI);
    }

    @Override
    public void processElement(Double value, Context ctx, Collector<StreamSlice> out) {
        // FIXME: The reader group exists, but the readerGroup.getStreamCuts() call does not provide updated StreamCuts.
        if (value < 0 && startStreamCut == null) {
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(StreamBookmarker.READER_GROUP_NAME);
            startStreamCut = readerGroup.getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM));
            LOG.warn("Start bookmarking a stream slice at: {}.", startStreamCut);
        } else if (value >= 0 && startStreamCut != null) {
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(StreamBookmarker.READER_GROUP_NAME);
            StreamSlice slice = new StreamSlice(startStreamCut,
                    readerGroup.getStreamCuts().get(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM)));
            LOG.warn("Finish bookmarking a stream slice at: {}.", slice);
            out.collect(slice);
            startStreamCut = null;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        readerGroupManager.close();
    }
}

class EventRouter implements PravegaEventRouter<StreamSlice> {

    @Override
    public String getRoutingKey(StreamSlice event) {
        // Ordering - events with the same routing key will always be read in the order they were written.
        return "SameRoutingKey";
    }
}
