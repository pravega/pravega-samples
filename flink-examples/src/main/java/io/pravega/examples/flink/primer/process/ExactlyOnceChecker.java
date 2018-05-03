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

import com.sun.org.apache.bcel.internal.generic.DUP;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.examples.flink.primer.datatype.IntegerEvent;
import io.pravega.examples.flink.primer.util.FlinkPravegaHelper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

/*
 * Parameters
 *     -stream <pravega_scope>/<pravega_stream>, e.g., myscope/stream1
 *     -controller tcp://<controller_host>:<port>, e.g., tcp://localhost:9090
 *
 */
public class ExactlyOnceChecker {

    // read data from the time when the program starts
    //private static final long readStartTimeMillis = Instant.now().toEpochMilli();
    private static final long readStartTimeMillis = 0L;
    private static final String defaultStream = "myscope/mystream";

    private static Set<IntegerEvent> checker = new HashSet<>();
    private static List<IntegerEvent> duplicates = new ArrayList<IntegerEvent>();

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceChecker.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ExactlyOnce checker ...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // create Pravega helper utility for Flink using the input paramaters
        FlinkPravegaHelper helper = new FlinkPravegaHelper(params);

        // get the Pravega stream from the input parameters
        StreamId streamId = helper.getStreamFromParam("stream", defaultStream);

        // create the Pravega stream if not existed.
        helper.createStream(streamId);

        // initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        FlinkPravegaReader<IntegerEvent> reader = helper.newReader(
                streamId,
                readStartTimeMillis,
                IntegerEvent.class
        );

        DataStream<IntegerEvent> dataStream = env
                .addSource(reader)
                .setParallelism(1);

        // create output stream to data read from Pravega
        //dataStream.print();

        DataStream<DuplicateEvent> duplicateStream = dataStream.flatMap(new FlatMapFunction<IntegerEvent, DuplicateEvent>() {
            @Override
            public void flatMap(IntegerEvent event, Collector<DuplicateEvent> out) throws Exception {

                if (event.isStart()) {
                    // clear checker when the beginning of stream marker arrives
                    checker.clear();
                    duplicates.clear();
                    System.out.println("\n============== Checker starts ===============");
                }
                if (event.isEnd()) {
                    if (duplicates.size() == 0) {
                        System.out.println("No duplicate found. EXACTLY_ONCE!");
                    } else {
                        System.out.println("Found duplicates");
                    }
                    System.out.println("============== Checker ends  ===============\n");
                }
                if (checker.contains(event)) {
                    duplicates.add(event);
                    DuplicateEvent dup = new DuplicateEvent(event.getValue());
                    System.out.println(dup);
                    out.collect(dup);
                } else {
                    checker.add(event);
                }
            }
        });

        // create output sink to print duplicates
        //duplicateStream.print();

        // execute within the Flink environment
        env.execute("ExactlyOnceChecker");

        LOG.info("Ending ExactlyOnceChecker...");
    }

    private static class DuplicateEvent {
       private long value;

       DuplicateEvent(long value ) {
           this.value = value;
       }

       @Override
       public String toString() {
           return "Duplicate event: " + this.value;
       }
    }

}
