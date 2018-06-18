/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flink.ourboros;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema;
import io.pravega.connectors.flink.serialization.JsonRowSerializationSchema;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * The Ourboros application creates a cyclicality by acting as both a source and sink.
 */
public class Ourboros {

    private static final Logger LOG = LoggerFactory.getLogger(Ourboros.class);

    private static final RowTypeInfo TUPLE2 = new RowTypeInfo(Types.STRING, Types.LONG);

    private static JsonRowSerializationSchema SERIALIZER = new JsonRowSerializationSchema(TUPLE2.getFieldNames());
    private static JsonRowDeserializationSchema DESERIALIZER = new JsonRowDeserializationSchema(TUPLE2);

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope("examples");

        // resolve the qualified name of the stream
        String streamName = params.get("input", "ourboros-" + UUID.randomUUID());
        Stream stream = pravegaConfig.resolve(streamName);

        bootstrap(pravegaConfig, stream,
                generateKeys(params.getInt("k", 1)),
                ScalingPolicy.fixed(params.getInt("segments", 1)));

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // read
        DataStream<Row> head = env.addSource(FlinkPravegaReader.<Row>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(DESERIALIZER)
                .withEventReadTimeout(Time.seconds(5))
                .build(), "head");

        head.print();
        head.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                LOG.trace("Read: {}", value);
            }
        });

        // transform
        DataStream<Row> transformed = head
                .flatMap(new TransformationFunction(params.getDouble("growth", 0.0))).name("transform");

        // write
        transformed.addSink(FlinkPravegaWriter.<Row>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withSerializationSchema(SERIALIZER)
                .withEventRouter(row -> (String) row.getField(0))
                .build())
                .name("tail");

        env.execute("Ourboros");
    }

    private static Iterator<String> generateKeys(int numKeys) {
        return IntStream.rangeClosed(1, numKeys).mapToObj(i -> "K" + i).iterator();
    }

    private static void bootstrap(PravegaConfig pravegaConfig, Stream stream, Iterator<String> keys, ScalingPolicy scalingPolicy) {

        // create the stream (if necessary)
        boolean streamIsNew;
        try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
            streamManager.createScope(stream.getScope());
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(scalingPolicy)
                    .build();
            streamIsNew = streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        }

        if (streamIsNew) {
            // kick-off the circulatory process by writing some events
            try (ClientFactory clientFactory = ClientFactory.withScope(stream.getScope(), pravegaConfig.getClientConfig());
                 EventStreamWriter<Row> writer = clientFactory.createEventWriter(stream.getStreamName(),
                         new FlinkSerializer<>(SERIALIZER),
                         EventWriterConfig.builder().build())) {

                List<CompletableFuture> outstandingWrites = new ArrayList<>();
                keys.forEachRemaining(key -> {
                    Row row = Row.of(key, 1L);
                    outstandingWrites.add(writer.writeEvent(key, row));
                });
                CompletableFuture.allOf(outstandingWrites.toArray(new CompletableFuture[0])).join();
            }
        }
    }

    private static class TransformationFunction extends RichFlatMapFunction<Row, Row> {
        private static final Random RANDOM = new Random(42L);
        private final double growthProbability;

        public TransformationFunction(double growthProbability) {
            this.growthProbability = growthProbability;
        }

        @Override
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            Row next = Row.copy(value);
            next.setField(1, (Long) next.getField(1) + 1);
            out.collect(next);
            if (RANDOM.nextDouble() < growthProbability) {
                out.collect(next);
            }
        }
    }

    /**
     * A Pravega {@link Serializer} that wraps around a Flink {@link SerializationSchema}.
     *
     * @param <T> The type of the event.
     */
    public static final class FlinkSerializer<T> implements Serializer<T>, Serializable {

        private final SerializationSchema<T> serializationSchema;

        public FlinkSerializer(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
        }

        @Override
        public ByteBuffer serialize(T value) {
            return ByteBuffer.wrap(serializationSchema.serialize(value));
        }

        @Override
        public T deserialize(ByteBuffer buffer) {
            throw new IllegalStateException("deserialize() called within a serializer");
        }
    }
}

