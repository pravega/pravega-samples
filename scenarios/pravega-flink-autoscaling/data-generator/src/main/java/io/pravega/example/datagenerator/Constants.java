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
package io.pravega.example.datagenerator;

/**
 * Defines a handful of constants used by the workload generator.
 */
public class Constants {
    protected static final String DEFAULT_SCOPE = "test";
    protected static final String DEFAULT_STREAM_NAME = "stream";
    protected static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
    protected static final int DEFAULT_STREAM_SCALING_RATE = 1000;
}
