/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.turbineheatprocessor;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TurbineHeatProcessor {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope("examples");

        // ensure that the scope and stream exist
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get("input", "turbineHeatTest"),
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1); // required since on a multi core CPU machine, the watermark is not advancing due to idle sources and causing window not to trigger

        // 1. read and decode the sensor events from a Pravega stream
        FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(String.class, new JavaSerializer<>()))
                .build();
        DataStream<SensorEvent> events = env.addSource(source, "input").map(new SensorMapper()).name("events");

        // 2. extract timestamp information to support 'event-time' processing
        SingleOutputStreamOperator<SensorEvent> timestamped = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorEvent>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(SensorEvent element) {
                return element.getTimestamp();
            }
        });

        // 3. summarize the temperature data for each sensor
        SingleOutputStreamOperator<SensorAggregate> summaries = timestamped
                .keyBy("sensorId")
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(8)))
                .fold(null, new SensorAggregator()).name("summaries");

        // 4. save to HDFS and print to stdout.  Refer to the TaskManager's 'Stdout' view in the Flink UI.
        summaries.print().name("stdout");
        if (params.has("output")) {
            summaries.writeAsCsv(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("TurbineHeatProcessor_" + stream);
    }

    private static class SensorMapper implements MapFunction<String,SensorEvent> {
        @Override
        public SensorEvent map(String value) {
            String[] tokens = value.split(", ");
            return new SensorEvent(
                Long.parseLong(tokens[0]),
                Integer.parseInt(tokens[1]),
                tokens[2],
                Float.parseFloat(tokens[3])
            );
        }
    }

    private static class SensorAggregator implements FoldFunction<SensorEvent,SensorAggregate> {
        @Override
        public SensorAggregate fold(SensorAggregate accumulator, SensorEvent evt) throws Exception {
            if (accumulator == null) {
                return new SensorAggregate(evt.getTimestamp(), evt.getSensorId(), evt.getLocation(),
                        evt.getTemp(), evt.getTemp());
            }
            return new SensorAggregate(accumulator.getStartTime(), evt.getSensorId(), evt.getLocation(),
                    Math.min(evt.getTemp(), accumulator.getTempMin()),
                    Math.max(evt.getTemp(), accumulator.getTempMax())
            );
        }
    }
}