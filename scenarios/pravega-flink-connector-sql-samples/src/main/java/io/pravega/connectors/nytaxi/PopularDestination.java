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
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.net.URI;

import static io.pravega.connectors.nytaxi.common.Constants.*;

/**
 * Identify the popular destination based on total trips that exceeds a given threshold for specific time window
 */

@Slf4j
public class PopularDestination {

    public void findPopularDestination(String ... args) {

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        final String scope = params.get("scope", DEFAULT_SCOPE);
        final String stream = params.get("stream", DEFAULT_STREAM);
        final String controllerUri = params.get("controllerUri", DEFAULT_CONTROLLER_URI);
        final int limit = params.getInt("threshold", DEFAULT_POPULAR_DEST_THRESHOLD);

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scope);

        TableSchema tableSchema = TripRecord.getTableSchema();

        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(scope + "/" + stream)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withRowtimeAttribute("pickupTime",
                        new ExistingField("pickupTime"),
                        new BoundedOutOfOrderTimestamps(30000L))
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

        String query =

                "SELECT " +
                "destLocationId, wstart, wend, cnt " +
                "FROM " +
                "(SELECT " +
                "destLocationId, " +
                "HOP_START(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart, " +
                "HOP_END(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend, " +
                "COUNT(destLocationId) AS cnt " +
                "FROM " +
                "(SELECT " +
                "pickupTime, " +
                "destLocationId " +
                "FROM TaxiRide) " +
                "GROUP BY destLocationId, HOP(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)) " +
                "WHERE cnt > " + limit;

        Table results = tEnv.sqlQuery(query);

        tEnv.toAppendStream(results, Row.class).print();

        try {
            env.execute("Popular-Destination");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }

    }
    public static void main(String... args) {
        PopularDestination popularDestination = new PopularDestination();
        popularDestination.findPopularDestination(args);
    }
}