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
import io.pravega.connectors.flink.table.descriptors.Pravega;
import io.pravega.connectors.nytaxi.common.TripRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Identify the popular taxi vendor based on total trips travelled on specific window interval.
 */

@Slf4j
public class PopularTaxiVendor extends AbstractHandler {

    public PopularTaxiVendor (String ... args) {
        super(args);
    }

    @Override
    public void handleRequest() {

        Schema schema = TripRecord.getSchemaWithPickupTimeAsRowTime();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );

        Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .forStream(Stream.of(getScope(), getStream()).getScopedName())
                .withPravegaConfig(getPravegaConfig());

        tEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema)
                .inAppendMode()
                .registerTableSource("TaxiRide");

        String fields = "vendorId, pickupTime, startLocationId, destLocationId, startLocationBorough, startLocationZone, destLocationBorough, destLocationZone";

        Table popularRides = tEnv
                .from("TaxiRide")
                .select(fields)
                .window(Slide.over("15.minutes").every("5.minutes").on("pickupTime").as("w"))
                .groupBy("vendorId, w")
                .select("vendorId, w.start AS start, w.end AS end, count(vendorId) AS cnt");

        tEnv.toAppendStream(popularRides, Row.class).print();

        try {
            env.execute("Popular-Taxi-Vendor");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }
    }
}