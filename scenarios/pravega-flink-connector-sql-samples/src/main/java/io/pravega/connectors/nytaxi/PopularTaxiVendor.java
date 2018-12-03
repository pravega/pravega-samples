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

import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.nytaxi.common.TripRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.net.URI;

import static io.pravega.connectors.nytaxi.common.Constants.*;

/**
 * Identify the popular taxi vendor based on total trips travelled on specific window interval
 */

@Slf4j
public class PopularTaxiVendor {

    public void findPopularVendor(String... args) {
        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        final String scope = params.get("scope", DEFAULT_SCOPE);
        final String stream = params.get("stream", DEFAULT_STREAM);
        final String controllerUri = params.get("controllerUri", DEFAULT_CONTROLLER_URI);

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scope);

        TableSchema tableSchema = TripRecord.getTableSchema();

        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(scope + "/" + stream)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withRowtimeAttribute("pickupTime", new ExistingField("pickupTime"), new BoundedOutOfOrderTimestamps(30000L))
                .withSchema(tableSchema)
                .withReaderGroupScope(scope)
                .build();

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // create a TableEnvironment
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerTableSource("TaxiRide", source);

        String fields = "vendorId, pickupTime, startLocationId, destLocationId, startLocationBorough, startLocationZone, destLocationBorough, destLocationZone";

        Table popularRides = tEnv
                .scan("TaxiRide")
                .select(fields)
                .window(Slide.over("15.minutes").every("5.minutes").on("pickupTime").as("w"))
                .groupBy("vendorId, w")
                .select("vendorId, w.start AS start, w.end AS end, count(vendorId) AS cnt")
                .select("vendorId, start, end, cnt");

        tEnv.toAppendStream(popularRides, Row.class).print();

        try {
            env.execute("Popular-Taxi-Vendor");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }

    }

    public static void main(String... args) {
        PopularTaxiVendor popularTaxiVendor = new PopularTaxiVendor();
        popularTaxiVendor.findPopularVendor(args);
    }
}