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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SliceProcessor.class);

    public static void main(String[] args) throws Exception {
        // Initialize the parameter utility tool in order to retrieve input parameters.
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // Create the Pravega source to read the StreamSlice objects stored by StreamBookmarker.
        FlinkPravegaReader<StreamSlice> reader = FlinkPravegaReader.<StreamSlice>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(Stream.of(Constants.DEFAULT_SCOPE, Constants.STREAMCUTS_STREAM))
                .withDeserializationSchema(PravegaSerialization.deserializationFor(StreamSlice.class))
                .build();

        // Initialize the Flink execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // At the moment, just print the received stream cut pairs (we will execute batch jobs on slices in the future).
        DataStream<StreamSlice> dataStream = env.addSource(reader)
                                              .map(s -> {
                                                  System.err.println(s);
                                                  return s;
                                              })
                                              .setParallelism(1);

        // execute within the Flink environment
        env.execute("SliceProcessor");

        LOG.info("Ending SliceProcessor...");
    }
}
