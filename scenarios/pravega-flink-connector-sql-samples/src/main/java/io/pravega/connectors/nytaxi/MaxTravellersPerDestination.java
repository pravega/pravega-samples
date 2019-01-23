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
import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import io.pravega.connectors.nytaxi.common.TripRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

/**
 * Find maximum number of travellers who travelled to a destination point for a given window interval.
 */

@Slf4j
public class MaxTravellersPerDestination extends AbstractHandler {

    public MaxTravellersPerDestination (String ... args) {
        super(args);
    }

    @Override
    public void handleRequest() {

        TableSchema tableSchema = TripRecord.getTableSchema();

        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(Stream.of(getScope(), getStream()).getScopedName())
                .withPravegaConfig(getPravegaConfig())
                .failOnMissingField(true)
                .withRowtimeAttribute("dropOffTime", new ExistingField("dropOffTime"), new BoundedOutOfOrderTimestamps(30000L))
                .withSchema(tableSchema)
                .withReaderGroupScope(getScope())
                .build();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        // create a TableEnvironment
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerTableSource("TaxiRide", source);

        String fields = "passengerCount, dropOffTime, destLocationZone";

        Table noOfTravelersPerDest = tEnv
                .scan("TaxiRide")
                .select(fields)
                .window(Tumble.over("1.hour").on("dropOffTime").as("w"))
                .groupBy("destLocationZone, w")
                .select("destLocationZone, w.start AS start, w.end AS end, count(passengerCount) AS cnt");

        tEnv.toAppendStream(noOfTravelersPerDest, Row.class).print();

        try {
            env.execute("Max-Travellers-Per-Destination");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }
    }
}