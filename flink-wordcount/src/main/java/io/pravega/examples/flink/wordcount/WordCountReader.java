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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    // Application parameters

    // the Pravega stream that the incoming data will be read from
    //   the input parameter name
    //   the default Pravega stream - scope/name
    private static final String STREAM_PARAMETER = "stream";
    private static final String DEFAULT_STREAM = "myscope/wordcount";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountReader...");

        // the following code snippet will setup and initialize a Pravega stream

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // create pravega helper utility for Flink using the input paramaters
        FlinkPravegaParams pravega = new FlinkPravegaParams(params);

        // get the Pravega stream information from the input parameters
        StreamId streamId = pravega.getStreamFromParam(STREAM_PARAMETER, DEFAULT_STREAM);

        // create the pravega stream itself using the stream ID
        pravega.createStream(streamId);

        // initialize up the execution environment for Flink to perform streaming
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega stream reader
        long startTime = 0;
        FlinkPravegaReader<WordCount> flinkPravegaReader = pravega.newReader(streamId, startTime, WordCount.class);

        // If needed - add the below for example on creating checkpoint
        // long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();
        // env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        //

        // add the Pravega reader as the data source
        DataStream<WordCount> dataStream = env.addSource(flinkPravegaReader);

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("WordCountReader");

        LOG.info("Ending WordCountReader...");
    }

}
