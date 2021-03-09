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
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

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

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createTableDdl("WATERMARK FOR pickupTime AS pickupTime - INTERVAL '30' SECONDS", "popular-vendor"));

        Table popularRides = tEnv
                .from("TaxiRide")
                .select($("vendorId"), $("pickupTime"))
                .window(Slide.over(lit(15).minutes()).every(lit(5).minutes()).on($("pickupTime")).as("w"))
                .groupBy($("vendorId"), $("w"))
                .select($("vendorId"), $("w").start().as("start"), $("w").end().as("end"), $("vendorId").count().as("cnt"));

        tEnv.toAppendStream(popularRides, Row.class).print();

        try {
            env.execute("Popular-Taxi-Vendor");
        } catch (Exception e) {
            log.error("Application Failed", e);
        }
    }
}