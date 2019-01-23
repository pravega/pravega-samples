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
package io.pravega.connectors.nytaxi.common;

public class Constants {

    public static final String TRIP_DATA = "yellow_tripdata_2018-01-segment.csv.gz";
    public static final String ZONE_LOOKUP_DATA = "taxi_zone_lookup.csv.gz";

    public static final String DEFAULT_SCOPE = "taxi";
    public static final String DEFAULT_STREAM = "trip";
    public static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    public static final int DEFAULT_NO_SEGMENTS = 3;
    public static final boolean CREATE_STREAM = true;

    public static final int DEFAULT_POPULAR_DEST_THRESHOLD = 20;

}
