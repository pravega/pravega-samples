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
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.nytaxi.common.Helper;
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_SCOPE;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_STREAM;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_CONTROLLER_URI;
import static io.pravega.connectors.nytaxi.common.Constants.CREATE_STREAM;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_POPULAR_DEST_THRESHOLD;
import static io.pravega.connectors.nytaxi.common.Constants.DEFAULT_NO_SEGMENTS;

@Data
public abstract class AbstractHandler {

    private final String scope;
    private final String stream;
    private final String controllerUri;
    private final boolean create;
    private final int limit;

    public AbstractHandler(String ... args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        this.scope = params.get("scope", DEFAULT_SCOPE);
        this.stream = params.get("stream", DEFAULT_STREAM);
        this.controllerUri = params.get("controllerUri", DEFAULT_CONTROLLER_URI);
        this.create = params.getBoolean("create-stream", CREATE_STREAM);
        this.limit = params.getInt("threshold", DEFAULT_POPULAR_DEST_THRESHOLD);
    }

    public PravegaConfig getPravegaConfig() {
        return  PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(controllerUri))
                .withDefaultScope(scope);
    }

    public void createStream() {
        Stream taxiStream = Stream.of(getScope(), getStream());
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(getControllerUri())).build();

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(DEFAULT_NO_SEGMENTS))
                .build();

        Helper helper = new Helper();
        helper.createStream(taxiStream, clientConfig, streamConfiguration);
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return env;
    }

    public String createTableDdl(String watermarkDdl, String readerGroupName) {
        return String.format(
                "CREATE TABLE TaxiRide (%n" +
                        "  rideId INT,%n" +
                        "  vendorId INT,%n" +
                        "  pickupTime TIMESTAMP(3),%n" +
                        "  dropOffTime TIMESTAMP(3),%n" +
                        "  passengerCount INT,%n" +
                        "  tripDistance FLOAT,%n" +
                        "  startLocationId INT,%n" +
                        "  destLocationId INT,%n" +
                        "  startLocationBorough STRING,%n" +
                        "  startLocationZone STRING,%n" +
                        "  startLocationServiceZone STRING,%n" +
                        "  destLocationBorough STRING,%n" +
                        "  destLocationZone STRING,%n" +
                        "  destLocationServiceZone STRING,%n" +
                        "  %s" +
                        ") with (%n" +
                        "  'connector' = 'pravega',%n" +
                        "  'controller-uri' = '%s',%n" +
                        "  'scope' = '%s',%n" +
                        "  'scan.execution.type' = '%s',%n" +
                        "  'scan.reader-group.name' = '%s',%n" +
                        "  'scan.streams' = '%s',%n" +
                        "  'format' = 'json'%n" +
                        ")",
                watermarkDdl,
                controllerUri,
                scope,
                "streaming",
                readerGroupName,
                stream,
                stream);
    }

    public abstract void handleRequest();
}
