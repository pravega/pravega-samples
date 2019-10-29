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
package io.pravega.example.flink.watermark;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {

    protected static final String CONTROLLER_ADDRESS = "tcp://localhost:9090";
    protected static final String CONTROLLER_ADDRESS_PARAM = "controller";
    protected static final int SENSOR_NUMBER = 3;
    // event number for each sensor
    protected static final int EVENTS_NUMBER = 10000;
    // ensure it covers a whole period of sine
    protected static final double EVENT_VALUE_INCREMENT = 0.01 * Math.PI;
    // one second for each event
    protected static final long EVENT_TIME_PERIOD = 1L;
    // Unix timestamp for 2017/7/14 10:40:00
    protected static final long STARTING_EVENT_TIME = 1500000000L;
    protected static final int WRITER_SLEEP_MS = 100;

    protected static final int PARALLELISM = 3;
    protected static final String INPUT_STREAM_PARAM = "input";
    protected static final String OUTPUT_STREAM_PARAM = "output";
    protected static final String WINDOW_LENGTH_PARAM = "window";
    protected static final String DEFAULT_SCOPE = "watermark-examples";
    protected static final String RAW_DATA_STREAM = "raw-data";
    protected static final String OUTPUT_STREAM = "output";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final int DEFAULT_PORT = 9999;
}
