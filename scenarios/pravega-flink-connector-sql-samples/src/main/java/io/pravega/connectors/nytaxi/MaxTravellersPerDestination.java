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
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.net.URI;

import static io.pravega.connectors.nytaxi.common.Constants.*;

/**
 * Find maximum number of travellers who travelled to a destination point for a given window interval
 */

@Slf4j
public class MaxTravellersPerDestination {

    public void findMaxTravellers(String ... args) {
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
                .withRowtimeAttribute("dropOffTime", new ExistingField("dropOffTime"), new BoundedOutOfOrderTimestamps(30000L))
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

        String fields = "passengerCount, dropOffTime, destLocationZone";

        Table popularRides = tEnv
                .scan("TaxiRide")
                .select(fields)
                .window(Tumble.over("1.hour").on("dropOffTime").as("w"))
                .groupBy("destLocationZone, w")
                .select("destLocationZone, w.start AS start, w.end AS end, count(passengerCount) AS cnt");
        //.select("destLocationZone, cnt");

        tEnv.toAppendStream(popularRides, Row.class).print();

        try {
            env.execute("Max-Travellers-Per-Destination");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }

    }

    public static void main(String... args) {
        MaxTravellersPerDestination maxTravellersPerDestination = new MaxTravellersPerDestination();
        maxTravellersPerDestination.findMaxTravellers(args);
    }
}