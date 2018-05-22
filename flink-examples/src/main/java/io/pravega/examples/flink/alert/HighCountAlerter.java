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
package io.pravega.examples.flink.alert;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.examples.flink.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * This application has the following input parameters
 *     stream - Pravega stream name to write to
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is processed in pravega flink connector
 */
public class HighCountAlerter {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(HighCountAlerter.class);

    // The application reads data from specified Pravega stream and once every ALERT_INTERVAL (2 seconds)
    // counts the number of 500 responses in the last ALERT_WINDOW (30 seconds), and generates
    // alert when the counts exceed ALERT_THRESHOLD (6).

    public static void main(String[] args) throws Exception {
        LOG.info("Starting HighErrorAlerter...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(params.get(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE));

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<String> reader = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(PravegaSerialization.deserializationFor(String.class))
                .build();

        // add the Pravega reader as the data source
        DataStream<String> inputStream = env.addSource(reader);

        // create an output sink to stdout for verification
        //inputStream.print();

        // transform logs
        DataStream<AccessLog> dataStream = inputStream.map(new ParseLogData());

        // create an output sink to stdout for verification
        //dataStream.print();

        // get responses and their counts
        DataStream<ResponseCount> countStream =
            dataStream.flatMap(new FlatMapFunction<AccessLog, ResponseCount>() {
                @Override
                public void flatMap(AccessLog value, Collector<ResponseCount> out) throws Exception {
                    out.collect(new ResponseCount(value.getStatus(), 1));
                }
            }).filter((FilterFunction<ResponseCount>) count -> {
                  return count.response.equals("500");
              }).keyBy("response")
              .timeWindow(Time.seconds(Constants.ALERT_WINDOW), Time.seconds(Constants.ALERT_INTERVAL))
              .sum("count");

        // create an output sink to stdout for verification
        countStream.print();

        // create alert pattern
        Pattern<ResponseCount,?> pattern500 = Pattern.<ResponseCount>begin("500pattern")
            .where(new SimpleCondition<ResponseCount>() {
                @Override
                public boolean filter(ResponseCount value) throws Exception {
                    return value.count >= Constants.ALERT_THRESHOLD;
                }
            });

        PatternStream<ResponseCount> patternStream = CEP.pattern(countStream, pattern500);

        DataStream<Alert> alertStream = patternStream.select(
            new PatternSelectFunction<ResponseCount, Alert>() {
                @Override
                public Alert select(Map<String, List<ResponseCount>> pattern) throws Exception {
                    ResponseCount count = pattern.get("500pattern").get(0);
                    return new Alert(count.response, count.count, "High 500 responses");
                }
            });

        // create an output sink to stdout for verification
        alertStream.print();


        // execute within the Flink environment
        env.execute("HighCountAlerter");

        LOG.info("Ending HighCountAlerter...");
    }

    //Parse the incoming streams & convert into Java PoJos
    private static class ParseLogData implements MapFunction<String, AccessLog>{
        public AccessLog map(String value) throws Exception {
            return AccessLog.toAccessLog(value);
        }
    }

    // Data type access status count
    public static class ResponseCount {

        public String response;
        public long count;

        public ResponseCount() {}

        public ResponseCount(String status, long count) {
            this.response = status;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Response count: " + response + " : " + count;
        }
    }

    // Data type access status count
    public static class Alert {

        private String response;
        private long count;
        private String description;

        public Alert() {}

        public Alert(String response, long count, String description) {
            this.response = response;
            this.count = count;
            this.description = description;
        }

        @Override
        public String toString() {
            return description + ": " + response + " : " + count;
        }
    }

}
