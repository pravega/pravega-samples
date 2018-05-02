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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.*;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.examples.flink.Utils;
import io.pravega.examples.flink.primer.datatype.IntegerEvent;
import io.pravega.examples.flink.primer.datatype.Constants;
import io.pravega.examples.flink.primer.source.ThrottledIntegerEventProducer;
import io.pravega.examples.flink.primer.util.FailingMapper;
 import org.apache.flink.api.common.restartstrategy.RestartStrategies;
 import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/*
 * Parameters
 *     -scope  Pravega scope
 *     -stream Pravega stream
 *     -controller tcp://<controller_host>:<port>, e.g., tcp://localhost:9090
 *     -num-events <number of events to be generated>
 *
 */
public class ExactlyOnceWriter {
    private static final long checkpointIntervalMillis = 100;
    private static final Time txnTimeoutMillis = Time.milliseconds(30 * 1000);
    private static final Time txnGracePeriodMillis = Time.milliseconds(30 * 1000);
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

        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withControllerURI(URI.create(params.get(Constants.Default_URI_PARAM, Constants.Default_URI)))
                .withDefaultScope(params.get(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE));

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(checkpointIntervalMillis, CheckpointingMode.EXACTLY_ONCE)
                .setParallelism(1);

        // Restart flink job from last checkpoint once.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));


        // create the Pravega sink to write a stream of text

        FlinkPravegaWriter<IntegerEvent> writer = FlinkPravegaWriter.<IntegerEvent>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withEventRouter( new EventRouter())
                .withTxnTimeout(txnTimeoutMillis)
                .withTxnGracePeriod(txnGracePeriodMillis)
                .withWriterMode( exactlyOnce ? PravegaWriterMode.EXACTLY_ONCE : PravegaWriterMode.ATLEAST_ONCE )
                .withSerializationSchema(PravegaSerialization.serializationFor(IntegerEvent.class))
                .build();

        env
            .addSource(new ThrottledIntegerEventProducer(numEvents))
            .map(new FailingMapper<>(numEvents /2 ))  // simulate failure
            .addSink(writer)
            .setParallelism(2);

        // execute within the Flink environment
        env.execute("ExactlyOnce");


        LOG.info("Ending ExactlyOnce...");
    }

    public static class EventRouter implements PravegaEventRouter<IntegerEvent> {
        @Override
        public String getRoutingKey(IntegerEvent event) {
            return "fixedkey";
        }
    }

}
