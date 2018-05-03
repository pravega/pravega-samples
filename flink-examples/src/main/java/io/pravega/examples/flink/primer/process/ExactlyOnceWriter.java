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
package io.pravega.examples.flink.primer.process;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.examples.flink.primer.datatype.IntegerEvent;
import io.pravega.examples.flink.primer.source.ThrottledIntegerEventProducer;
import io.pravega.examples.flink.primer.util.FailingMapper;
import io.pravega.examples.flink.primer.util.FlinkPravegaHelper;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/*
 * Parameters
 *     -stream <pravega_scope>/<pravega_stream>, e.g., myscope/stream1
 *     -controller tcp://<controller_host>:<port>, e.g., tcp://localhost:9090
 *     -num-events <number of events to be generated>
 *
 */
public class ExactlyOnceWriter {
    private static final long checkpointIntervalMillis = 100;
    private static final long txnTimeoutMillis = 30 * 1000;
    private static final long txnGracePeriodMillis = 30 * 1000;
    private static final int defaultNumEvents = 50;
    private static final String defaultStream = "myscope/mystream";

    // read data from the time when the program starts
    //private static final long readStartTimeMillis = Instant.now().toEpochMilli();

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceWriter.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ExactlyOnce checker ...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        boolean exactlyOnce = Boolean.parseBoolean(params.get("exactlyonce", "true"));
        int numEvents = Integer.parseInt(params.get("num-events", String.valueOf(defaultNumEvents)));

        // create Pravega helper utility for Flink using the input paramaters
        FlinkPravegaHelper helper = new FlinkPravegaHelper(params);

        // get the Pravega stream from the input parameters
        StreamId streamId = helper.getStreamFromParam("stream", defaultStream);

        // create the Pravega stream if not existed.
        helper.createStream(streamId);

        // initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(checkpointIntervalMillis, CheckpointingMode.EXACTLY_ONCE)
                .setParallelism(1);

        // Restart flink job from last checkpoint once.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        FlinkPravegaWriter<IntegerEvent> writer = helper.newWriter(
                streamId,
                IntegerEvent.class,
                event -> "fixedkey",
                txnTimeoutMillis,
                txnGracePeriodMillis
        );
        if (exactlyOnce) {
            writer.setPravegaWriterMode(PravegaWriterMode.EXACTLY_ONCE);
        }

        env
            .addSource(new ThrottledIntegerEventProducer(numEvents))
            .map(new FailingMapper<>(numEvents /2 ))  // simulate failure
            .addSink(writer)
            .setParallelism(2)
            ;


        // execute within the Flink environment
        env.execute("ExactlyOnce");


        LOG.info("Ending ExactlyOnce...");
    }

    private static class IntSerializer implements SerializationSchema<Integer> {
        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }
}
