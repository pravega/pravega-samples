package io.pravega.example.flink.watermark.reader;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import io.pravega.example.flink.Utils;
import io.pravega.example.flink.watermark.Constants;
import io.pravega.example.flink.watermark.SensorData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkWatermarkReader {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(FlinkWatermarkReader.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Watermark Reader...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.INPUT_STREAM));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Using event time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);

        // create the Pravega source to read a stream of SensorData with Pravega watermark
        FlinkPravegaReader<SensorData> source = FlinkPravegaReader.<SensorData>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(PravegaSerialization.deserializationFor(SensorData.class))
                // provide a implementation of AssignerWithTimeWindows<T>
                .withTimestampAssigner(new LowerBoundAssigner<SensorData>() {
                    @Override
                    public long extractTimestamp(SensorData sensorData, long previousTimestamp) {
                        return sensorData.getTimestamp();
                    }
                })
                .build();

        DataStream<SensorData> dataStream = env.addSource(source).name("Pravega Stream")
                .setParallelism(Constants.PARALLELISM);

        // Calculating the average of each sensor in a 10 second period upon event-time clock.
        dataStream.keyBy("sensorId")
                .timeWindow(Time.seconds(10))
                .aggregate(new WindowAverage(), new WindowProcess())
                .print();

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("WordCountReader");

        LOG.info("Ending WordCountReader...");
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
            Tuple4<Integer, Long, Long, Double>, Tuple, TimeWindow> {

        @Override
        public void process(Tuple key, Context context, Iterable<Double> iterable,
                            Collector<Tuple4<Integer, Long, Long, Double>> collector) throws Exception {
            Double avg = iterable.iterator().next();
            int sensorId = key.getField(0);
            collector.collect(new Tuple4<>(sensorId, context.window().getStart(), context.window().getEnd(), avg));
        }
    }
}