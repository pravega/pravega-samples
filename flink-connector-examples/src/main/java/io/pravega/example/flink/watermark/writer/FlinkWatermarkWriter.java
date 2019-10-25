package io.pravega.example.flink.watermark.writer;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.example.flink.Utils;
import io.pravega.example.flink.watermark.Constants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Functionality
 *     - Read data from a socket(127.0.0.1:9999) and write it into a Pravega stream(watermark-examples/output) with watermark.
 */
public class FlinkWatermarkWriter {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(FlinkWatermarkWriter.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Watermark Writer...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.OUTPUT_STREAM));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Using event time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);

        // Get the data from socket and assign timestamp and a periodic watermark strategy onto the DataStream
        DataStream<String> dataStream = env.socketTextStream(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT)
                .uid("Socket Stream")
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>())
                .uid("Timestamps/Watermarks");

        // Register a Flink Pravega writer with watermark enabled
        FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .enableWatermark(true)
                .withEventRouter(event -> event)
                .withSerializationSchema(PravegaSerialization.serializationFor(String.class))
                .build();

        // Write into the sink
        dataStream.addSink(writer).name("Pravega Writer").uid("Pravega Writer");

        // execute within the Flink environment
        env.execute("Watermark Writer");
    }
}
