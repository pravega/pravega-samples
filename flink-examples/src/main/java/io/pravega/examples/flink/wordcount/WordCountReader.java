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
package io.pravega.examples.flink.wordcount;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.examples.flink.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * At a high level, WordCountReader reads from a Pravega stream, and prints 
 * the word count summary to the output. This class provides an example for 
 * a simple Flink application that reads streaming data from Pravega.
 *
 * This application has the following input parameters
 *     stream - Pravega stream name to read from
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is automatically used by the PravegaConfig class
 */
public class WordCountReader {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(WordCountReader.class);

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.

    // Application parameters
    //   stream - default myscope/wordcount
    //   controller - default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountReader...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig.fromParams(params);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(PravegaSerialization.deserializationFor(String.class))
                .build();

        // count each word over a 10 second time period
        DataStream<WordCount> dataStream = env.addSource(source)
                .flatMap(new WordCountReader.Splitter())
                .keyBy("word")
                .timeWindow(Time.seconds(10))
                .sum("count");

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("WordCountReader");

        LOG.info("Ending WordCountReader...");
    }

    // split data into word by space
    private static class Splitter implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String line, Collector<WordCount> out) throws Exception {
            for (String word: line.split(Constants.WORD_SEPARATOR)) {
                out.collect(new WordCount(word, 1));
            }
        }
    }

}
