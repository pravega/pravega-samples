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
package io.pravega.example.flink.watermark;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.serialization.PravegaSerializationSchema;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import io.pravega.example.flink.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Parameters
 *     --controller tcp://<controller_host>:<port>, e.g., tcp://localhost:9090
 *     --input input Stream name
 *     --output output Stream name
 *     --window event time window length in seconds
 */

public class EventTimeAverage {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(EventTimeAverage.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Event Time Average...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // Get or create the Pravega input/output stream
        Stream inputStream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.INPUT_STREAM_PARAM, Constants.RAW_DATA_STREAM));
        Stream outputStream = Utils.createStream(
                pravegaConfig,
                params.getRequired(Constants.OUTPUT_STREAM_PARAM));

        // Get window length (in seconds) from parameters
        long windowLength = params.getInt(Constants.WINDOW_LENGTH_PARAM, 10);

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the auto watermark interval to 2 seconds
        env.getConfig().setAutoWatermarkInterval(2000);

        // create the Pravega source to read a stream of SensorData with Pravega watermark
        FlinkPravegaReader<SensorData> source = FlinkPravegaReader.<SensorData>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(inputStream)
                .withDeserializationSchema(new PravegaDeserializationSchema<>(SensorData.class, new JavaSerializer<>()))
                // provide an implementation of AssignerWithTimeWindows<T>
                .withTimestampAssigner(new LowerBoundAssigner<SensorData>() {
                    @Override
                    public long extractTimestamp(SensorData sensorData, long previousTimestamp) {
                        return sensorData.getTimestamp();
                    }
                })
                .build();

        DataStream<SensorData> dataStream = env.addSource(source).name("Pravega Reader").uid("Pravega Reader")
                .setParallelism(Constants.PARALLELISM);

        // Calculate the average of each sensor in a 10 second period upon event-time clock.
        DataStream<SensorData> avgStream = dataStream
                .keyBy(SensorData::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowLength)))
                .aggregate(new WindowAverage(), new WindowProcess())
                .uid("Count Event-time Average");

        // Print to stdout for verification
        avgStream.print().uid("Print to Std. Out");

        // Register a Flink Pravega writer with watermark enabled
        FlinkPravegaWriter<SensorData> writer = FlinkPravegaWriter.<SensorData>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStream)
                // enable watermark propagation
                .enableWatermark(true)
                .withEventRouter(event -> String.valueOf(event.getSensorId()))
                .withSerializationSchema(new PravegaSerializationSchema<>(new JavaSerializer<>()))
                .build();

        // Print to pravega sink for further usage
        avgStream.addSink(writer).name("Pravega Writer").uid("Pravega Writer");

        // execute within the Flink environment
        env.execute("Sensor Event Time Average");

        LOG.info("Ending Event Time Average...");
    }

    private static class WindowAverage implements AggregateFunction<SensorData, Tuple2<Integer, Double>, Double> {

        @Override
        public Tuple2<Integer, Double> createAccumulator() {
            return new Tuple2<>(0, 0.0);
        }

        @Override
        public Tuple2<Integer, Double> add(SensorData sensorData, Tuple2<Integer, Double> acc) {
            return new Tuple2<>(acc.f0 + 1, acc.f1 + sensorData.getValue());
        }

        @Override
        public Double getResult(Tuple2<Integer, Double> acc) {
            return acc.f1 / acc.f0;
        }

        @Override
        public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> acc1, Tuple2<Integer, Double> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }

    private static class WindowProcess extends ProcessWindowFunction<Double,
            SensorData, Integer, TimeWindow> {

        @Override
        public void process(Integer key, Context context, Iterable<Double> iterable,
                            Collector<SensorData> collector) throws Exception {
            Double avg = iterable.iterator().next();
            // Set the event timestamp to the ending timestamp of the window
            long eventTime = context.window().getEnd();
            collector.collect(new SensorData(key, avg, eventTime));
        }
    }
}