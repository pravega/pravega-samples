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
    public static final String CONTROLLER_ADDRESS = "tcp://localhost:9090";
    public static final String CONTROLLER_ADDRESS_PARAM = "controller";
    public static final int EVENTS_NUMBER = 10000;
    public static final int PARALLELISM = 3;
    public static final String STREAM_PARAM = "stream";
    public static final String DEFAULT_SCOPE = "examples";
    public static final String INPUT_STREAM = "input";
    public static final String OUTPUT_STREAM = "output";
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9999;
}
