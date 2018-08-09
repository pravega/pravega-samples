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

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.example.flink.streamcuts.Constants;
import io.pravega.example.flink.streamcuts.SensorStreamSlice;
import io.pravega.example.flink.streamcuts.serialization.Tuple2DeserializationSchema;
import java.net.URI;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is first intended to read from a stream where stream slices are published by the StreamBookmarker process.
 * Then, upon a new stream slice received, this class runs a new batch job on that slice that represents a set of events
 * in the stream where DataProducer is storing events.
 */
public class SliceProcessor {

    private static final String READER_GROUP_NAME = "sliceProcessorReaderGroup" + System.currentTimeMillis();

    // The writer will contact with the Pravega controller to get information about segments.
    private static final URI pravegaControllerURI = URI.create("tcp://" + Constants.CONTROLLER_HOST + ":" + Constants.CONTROLLER_PORT);

    private static final long READER_TIMEOUT_MS = 600 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(SliceProcessor.class);

    public static void main(String[] args) throws Exception {
        // Initialize the parameter utility tool in order to retrieve input parameters.
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // We will read from the stream slices published by StreamBookmarker.
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                               .stream(Stream.of(Constants.DEFAULT_SCOPE, Constants.STREAMCUTS_STREAM))
                                                               .build();

        // Instantiate the reader group manager to create the reader group and the client factory to create readers.
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI);
             ClientFactory clientFactory = ClientFactory.withScope(Constants.DEFAULT_SCOPE, pravegaControllerURI)) {

            // Create the reader group to read the stream slices.
            readerGroupManager.createReaderGroup(READER_GROUP_NAME, readerGroupConfig);
            EventStreamReader<SensorStreamSlice> sliceReader = clientFactory.createReader("sliceReader", READER_GROUP_NAME,
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            EventRead<SensorStreamSlice> sliceToAnalyze;
            do {
                sliceToAnalyze = sliceReader.readNextEvent(READER_TIMEOUT_MS);

                // If we got a new stream slice to process, run a new batch job on it.
                if (sliceToAnalyze.getEvent() != null) {
                    LOG.info("Running batch job for slice: {}.", sliceToAnalyze.getEvent());
                    triggerBatchJobOnSlice(pravegaConfig, sliceToAnalyze.getEvent());
                }
            } while (sliceToAnalyze.isCheckpoint() || sliceToAnalyze.getEvent() != null);

            sliceReader.close();
        }
    }

    /**
     * This method triggers a new batch job on the events created by DataProducer that fall within the start and end
     * StreamCuts defined in the input SensorStreamSlice.
     *
     * @param pravegaConfig Configuration for batch job.
     * @param sensorStreamSlice Pair of StreamCuts that define the range of events to analyze for a sensor (the ones created by DataProducer).
     * @throws Exception
     */
    private static void triggerBatchJobOnSlice(PravegaConfig pravegaConfig, SensorStreamSlice sensorStreamSlice) throws Exception {
        // Initialize the Flink execution environment (this job will run in the job driver program).
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        // Instantiate a DataSet with the events defined in the slice.
        DataSet<Tuple2<Integer, Double>> sliceEvents = env.createInput(
                FlinkPravegaInputFormat.<Tuple2<Integer, Double>>builder()
                        .forStream(Stream.of(Constants.DEFAULT_SCOPE, Constants.PRODUCER_STREAM), sensorStreamSlice.getStart(), sensorStreamSlice.getEnd())
                        .withPravegaConfig(pravegaConfig)
                        .withDeserializationSchema(new Tuple2DeserializationSchema())
                        .build(),
                TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>(){})
        );

        // The batch job is simply to count the events in the slice.
        LOG.warn("Number of events in this slice for sensor " + sensorStreamSlice.getSensorId() + ": " +
                sliceEvents.filter(eventTuple -> eventTuple.f0 == sensorStreamSlice.getSensorId())
                           .count());
    }
}
