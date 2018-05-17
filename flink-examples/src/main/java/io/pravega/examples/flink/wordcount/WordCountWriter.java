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
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.examples.flink.Utils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

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
    //   host - host running netcat, default 127.0.0.1
    //   port - port on which netcat listens, default 9999
    //   stream - the Pravega stream to write data to, default examples/wordcount
    //   controller - the Pravega controller uri, default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountWriter...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // retrieve the socket host and port information to read the incoming data from
        String host = params.get(Constants.HOST_PARAM, Constants.DEFAULT_HOST);
        int port = Integer.parseInt(params.get(Constants.PORT_PARAM, Constants.DEFAULT_PORT));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> dataStream = env.socketTextStream(host, port);

        // create the Pravega sink to write a stream of text
        FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withEventRouter(new EventRouter())
                .withSerializationSchema(PravegaSerialization.serializationFor(String.class))
                .build();
        dataStream.addSink(writer).name("Pravega Stream");

        // create another output sink to print to stdout for verification
        dataStream.print().name("stdout");

        // execute within the Flink environment
        env.execute("WordCountWriter");

        LOG.info("Ending WordCountWriter...");
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<String> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(String event) {
            return "SameRoutingKey";
        }
    }
}
