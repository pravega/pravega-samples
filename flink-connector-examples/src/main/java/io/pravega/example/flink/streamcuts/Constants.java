/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 */
package io.pravega.example.flink.streamcuts;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {
    public static final String CONTROLLER_ADDRESS = "tcp://localhost:9090";
    public static final String CONTROLLER_ADDRESS_PARAM = "controller";
    public static final int NUM_EVENTS = 10000;
    public static final String NUM_EVENTS_PARAM = "num-events";
    public static final String DEFAULT_SCOPE = "examples";
    public static final String PRODUCER_STREAM = "streamcuts-producer";
    public static final String STREAMCUTS_STREAM = "streamcuts-results";
    public static final int PARALLELISM = 3;
}
