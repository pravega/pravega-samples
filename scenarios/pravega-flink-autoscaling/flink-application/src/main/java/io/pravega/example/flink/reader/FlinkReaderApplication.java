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
package io.pravega.example.flink.reader;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink application that reads data from the SinusoidalWorkloadGenerator and performs some
 * compute intensive operations on a per-event basis to simulate non-trivial computations.
 */
public class FlinkReaderApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkReaderApplication.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Rescalable demo...");

        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        Stream stream = Stream.of(Constants.DEFAULT_SCOPE, Constants.DEFAULT_STREAM);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.disableOperatorChaining();
        env.enableCheckpointing(30_000L);

        FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(new SimpleStringSchema())
                .build();

        DataStream<String> dataStream = env.addSource(source).name("Pravega Stream");

        dataStream.flatMap(new ThroughputLogger(16, params.getLong("logfreq", 10_000)))
                .setMaxParallelism(1)
                .name("Throughput Logger");

        dataStream.rebalance()
                .map(new CPULoadMapper(params))
                .name("Expensive Computation")
                .addSink(new DiscardingSink<>())
                .name("Sink")
                .setParallelism(9999);
        env.execute("Auto-scaling Job");

    }

    private static class CPULoadMapper extends RichMapFunction<String, String> {
        private final ParameterTool params;
        private boolean firstMessage = false;

        public CPULoadMapper(ParameterTool params) {
            this.params = params;
        }

        // Let's waste some CPU cycles
        @Override
        public String map(String s) throws Exception {
            if (!firstMessage) {
                LOG.info("Received message " + s);
                firstMessage = true;
            }
            if (s.equals("fail")) {
                throw new RuntimeException("Artificial failure");
            }

            // Simulate some expensive computation here.
            double res = 0;
            for (int i = 0; i < params.getInt("iterations", 150); i++) {
                String[] dnaSequenceParts = s.split("\n");
                String seqId = dnaSequenceParts[0];
                int adenineCount = dnaSequenceParts[1].chars().reduce(0, (acc, c) -> c == 'A' ? acc + 1 : acc);
                int cytosineCount = dnaSequenceParts[1].chars().reduce(0, (acc, c) -> c == 'C' ? acc + 1 : acc);
                int guanineCount = dnaSequenceParts[1].chars().reduce(0, (acc, c) -> c == 'G' ? acc + 1 : acc);
                int thymineCount = dnaSequenceParts[1].chars().reduce(0, (acc, c) -> c == 'T' ? acc + 1 : acc);
            }
            return s + res;
        }
    }

    private static class ThroughputLogger<T> implements FlatMapFunction<T, Integer> {

        private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);
        private final int elementSize;
        private final long logfreq;
        private long totalReceived = 0;
        private long lastTotalReceived = 0;
        private long lastLogTimeMs = -1;

        public ThroughputLogger(int elementSize, long logfreq) {
            this.elementSize = elementSize;
            this.logfreq = logfreq;
        }

        @Override
        public void flatMap(T element, Collector<Integer> collector) throws Exception {
            totalReceived++;
            if (totalReceived % logfreq == 0) {
                // throughput over entire time
                long now = System.currentTimeMillis();

                // throughput for the last "logfreq" elements
                if (lastLogTimeMs == -1) {
                    // init (the first)
                    lastLogTimeMs = now;
                    lastTotalReceived = totalReceived;
                } else {
                    long timeDiff = now - lastLogTimeMs;
                    long elementDiff = totalReceived - lastTotalReceived;
                    double ex = (1000 / (double) timeDiff);
                    LOG.info(
                            "During the last {} ms, we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {}",
                            timeDiff,
                            elementDiff,
                            elementDiff * ex,
                            elementDiff * ex * elementSize / 1024 / 1024,
                            (totalReceived * elementSize) / 1024 / 1024 / 1024);
                    // reinit
                    lastLogTimeMs = now;
                    lastTotalReceived = totalReceived;
                }
            }
        }
    }
}
