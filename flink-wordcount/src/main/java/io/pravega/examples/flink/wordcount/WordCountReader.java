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

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
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
 *     stream - Pravega stream name to write to
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is processed in pravega flink connector
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

        // create pravega helper utility for Flink using the input paramaters
        // the controller param is processed in FlinkPravegaParams
        FlinkPravegaParams pravega = new FlinkPravegaParams(params);

        // get the Pravega stream from the input parameters
        StreamId streamId = pravega.getStreamFromParam(Constants.STREAM_PARAM,
                                                       Constants.DEFAULT_STREAM);

        // create the Pravega stream is not exists.
        pravega.createStream(streamId);

        // initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega stream reader
        long startTime = 0;
        FlinkPravegaReader<String> reader = pravega.newReader(streamId, startTime, String.class);

        // If needed - add the below for example on creating checkpoint
        // long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();
        // env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        //

        // add the Pravega reader as the data source
        DataStream<WordCount> dataStream = env.addSource(reader)
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
