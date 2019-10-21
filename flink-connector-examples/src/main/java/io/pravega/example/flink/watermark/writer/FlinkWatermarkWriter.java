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

        // set the socket information to read the incoming data from
        String host = Constants.DEFAULT_HOST;
        int port = Constants.DEFAULT_PORT;

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Using event time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);

        // count each word over a 10 second time period
        DataStream<String> dataStream = env.socketTextStream(host, port)
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .enableWatermark(true)
                .withSerializationSchema(PravegaSerialization.serializationFor(String.class))
                .build();

        dataStream.addSink(writer).name("Pravega Stream");

        // execute within the Flink environment
        env.execute("Watermark Writer");
    }
}
