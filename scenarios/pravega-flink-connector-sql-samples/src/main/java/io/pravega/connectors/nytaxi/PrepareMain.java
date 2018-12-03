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
package io.pravega.connectors.nytaxi;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.JsonRowSerializationSchema;
import io.pravega.connectors.nytaxi.common.Helper;
import io.pravega.connectors.nytaxi.common.TripRecord;
import io.pravega.connectors.nytaxi.common.ZoneLookup;
import io.pravega.connectors.nytaxi.source.TaxiDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static io.pravega.connectors.nytaxi.common.Constants.CREATE_STREAM;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_CONTROLLER_URI;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_NO_SEGMENTS;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_SCOPE;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_STREAM;
import static io.pravega.connectors.nytaxi.common.Constants.TRIP_DATA;
import static io.pravega.connectors.nytaxi.common.Constants.ZONE_LOOKUP_DATA;

@Slf4j
public class PrepareMain {

    public void prepareData(String... args) {
        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        final String scope = params.get("scope", DEFAULT_SCOPE);
        final String stream = params.get("stream", DEFAULT_STREAM);
        final String controllerUri = params.get("controllerUri", DEFAULT_CONTROLLER_URI);
        final boolean create = params.getBoolean("create-stream", CREATE_STREAM);

        if (create) {
            Stream taxiStream = Stream.of(scope, stream);
            ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerUri)).build();

            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scope(scope)
                    .streamName(stream)
                    .scalingPolicy(ScalingPolicy.fixed(DEFAULT_NO_SEGMENTS))
                    .build();

            Helper helper = new Helper();
            helper.createStream(taxiStream, clientConfig, streamConfiguration);
        }

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scope);

        Stream streamInfo = pravegaConfig.resolve(scope + "/" + stream);

        FlinkPravegaWriter<Row> writer = FlinkPravegaWriter.<Row>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(streamInfo)
                .withSerializationSchema(new JsonRowSerializationSchema(TripRecord.getFieldNames()))
                .withEventRouter(event -> String.valueOf(event.getField(6)))
                .build();

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Helper helper = new Helper();
        Map<Integer, ZoneLookup> zoneLookupRecordMap;
        try {
            zoneLookupRecordMap = helper.parseZoneData(ZONE_LOOKUP_DATA);
        } catch (IOException e) {
            log.error("failed to read zone-lookup data", e);
            return;
        }

        TaxiDataSource taxiDataSource = new TaxiDataSource(TRIP_DATA, zoneLookupRecordMap);

        DataStreamSource<TripRecord> streamSource = env.addSource(taxiDataSource);
        streamSource.name("source");

        DataStream mapper = streamSource.map((MapFunction<TripRecord, Row>) tripRecord -> TripRecord.transform(tripRecord));
        ((SingleOutputStreamOperator) mapper).name("transform");
        mapper.print();

        mapper.addSink(writer);

        try {
            env.execute("ingest");
        } catch (Exception e) {
            log.error("fail to ingest data", e);
        }

    }

    public static void main(String... args) {
        PrepareMain prepareMain = new PrepareMain();
        prepareMain.prepareData(args);
    }

}