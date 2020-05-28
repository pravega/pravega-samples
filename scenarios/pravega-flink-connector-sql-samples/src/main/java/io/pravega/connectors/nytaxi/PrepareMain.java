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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.nytaxi.common.Helper;
import io.pravega.connectors.nytaxi.common.TripRecord;
import io.pravega.connectors.nytaxi.common.ZoneLookup;
import io.pravega.connectors.nytaxi.source.TaxiDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

import static io.pravega.connectors.nytaxi.common.Constants.TRIP_DATA;
import static io.pravega.connectors.nytaxi.common.Constants.ZONE_LOOKUP_DATA;

@Slf4j
public class PrepareMain extends AbstractHandler {

    public PrepareMain (String ... args) {
        super(args);
    }

    @Override
    public void handleRequest() {

        if (isCreate()) {
            createStream();
        }

        Stream streamInfo = getPravegaConfig().resolve(Stream.of(getScope(), getStream()).getScopedName());

        TypeInformation<Row> typeInfo = (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(TripRecord.getTableSchema().toRowDataType());

        FlinkPravegaWriter<Row> writer = FlinkPravegaWriter.<Row>builder()
                .withPravegaConfig(getPravegaConfig())
                .forStream(streamInfo)
                .withSerializationSchema(new JsonRowSerializationSchema.Builder(typeInfo).build())
                .withEventRouter(event -> String.valueOf(event.getField(6)))
                .build();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

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
}