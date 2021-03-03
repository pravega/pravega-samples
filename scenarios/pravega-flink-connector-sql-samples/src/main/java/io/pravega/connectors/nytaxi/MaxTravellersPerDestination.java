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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

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

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createTableDdl("WATERMARK FOR dropOffTime AS dropOffTime - INTERVAL '30' SECONDS", "max-traveller"));

        Table noOfTravelersPerDest = tEnv
                .from("TaxiRide")
                .select($("passengerCount"), $("dropOffTime"), $("destLocationZone"))
                .window(Tumble.over(lit(1).hour()).on($("dropOffTime")).as("w"))
                .groupBy($("destLocationZone"), $("w"))
                .select($("destLocationZone"), $("w").start().as("start"), $("w").end().as("end"), $("passengerCount").count().as("cnt"));

        tEnv.toAppendStream(noOfTravelersPerDest, Row.class).print();

        try {
            env.execute("Max-Travellers-Per-Destination");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }
    }
}