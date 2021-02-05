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
package io.pravega.example.flink.primer.process;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.example.flink.Utils;
import io.pravega.example.flink.primer.datatype.Constants;
import io.pravega.example.flink.primer.datatype.IntegerEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

/*
 * Parameters
 *     -stream <pravega_scope>/<pravega_stream>, e.g., myscope/stream1
 *     -controller tcp://<controller_host>:<port>, e.g., tcp://localhost:9090
 *
 */
public class ExactlyOnceChecker {

    private static Set<IntegerEvent> checker = new HashSet<>();
    private static List<IntegerEvent> duplicates = new ArrayList<IntegerEvent>();

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceChecker.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ExactlyOnce checker ...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withControllerURI(URI.create(params.get(Constants.Default_URI_PARAM, Constants.Default_URI)))
                .withDefaultScope(params.get(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE));

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<IntegerEvent> reader = FlinkPravegaReader.<IntegerEvent>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(IntegerEvent.class, new JavaSerializer<>()))
                .build();

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
