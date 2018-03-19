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

import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
 * At a high level, WordCountWriter reads from an external network socket, 
 * transforms the data, and finally writes the data to a Pravega stream.
 *
 * This class provides an example for a simple Flink application that
 * writes streaming data to Pravega.
 *
 * This application has the following input parameters
 *     host   - hostname or IP Address of the external network source - 
                typically running the netcat utility on this host
 *     port   - port of the external network source
 *     stream - Pravega stream name to write to
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090.
 *                  Note that this parameter is processed in pravega flink connector
 */
public class WordCountWriter {

// Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(WordCountWriter.class);

    // Application parameters

    // Network host running the netcat utility
    //   the input parameter name
    //   the default host IP Address
    private static final String NW_HOST_PARAMETER = "host";
    private static final String DEFAULT_HOST_PARAMETER = "127.0.0.1";

    // Network port running the netcat utility
    //   the input parameter name
    //   the default port
    private static final String NW_PORT_PARAMETER = "port";
    private static final String DEFAULT_PORT_PARAMETER = "9999";

    // the Pravega stream that the incoming data will be written to
    //   the input parameter name
    //   the default Pravega stream - scope/name
    private static final String STREAM_PARAMETER = "stream";
    private static final String DEFAULT_STREAM = "examples/wordcount";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountWriter...");

        // the following code snippet will setup and initialize a Pravega stream

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // create pravega helper utility for Flink using the input paramaters
        FlinkPravegaParams pravega = new FlinkPravegaParams(params);

        // get the Pravega stream information from the input parameters
        StreamId streamId = pravega.getStreamFromParam(STREAM_PARAMETER, DEFAULT_STREAM);

        // create the pravega stream itself using the stream ID
        pravega.createStream(streamId);

        // retrieve the network host and port information to read the incoming data from
        String hostIP = params.get(NW_HOST_PARAMETER, DEFAULT_HOST_PARAMETER);
        int hostPort = Integer.parseInt(params.get(NW_PORT_PARAMETER, DEFAULT_PORT_PARAMETER));

        // initialize up the execution environment for Flink to perform streaming
        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // as an example, the below reads input from a network and summarizes the words 
        // every five seconds the code below sets up the following
        //   setup an incoming socket stream to read from a network host and port
        //   a sentence/line is read and split into words
        //   set a transformation window of 5 seconds to perform a basic count analytics for each word
        DataStream<WordCount> dataStream =
            see.socketTextStream(hostIP,hostPort).flatMap(new Splitter()).keyBy("word")
                                .timeWindow(Time.seconds(5))
                                .sum("count");

        // create the Pravega writer and add this as a sink for the events to be written to
        FlinkPravegaWriter<WordCount> writer = pravega.newWriter(streamId, WordCount.class, new EventRouter());
        dataStream.addSink(writer);

        // create another output sink to print to stdout for verification
        dataStream.print();

        // execute the above playbook within the Flink environment
        see.execute("WordCountWriter");

        LOG.info("Ending WordCountWriter...");
    }

    /*
     * splits the incoming stream in sentence/line form into individual words
     * creates each sord as an event of type WordCount
     */
    public static class Splitter implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String sentence, Collector<WordCount> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new WordCount(word, 1));
            }
        }
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<WordCount> {
        @Override
        public String getRoutingKey(WordCount event) {
            return event.getWord();
        }
    }

}
