/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.secure;

/**
 * Defines constants shared by classes in this package.
 */
public class Constants {
    static final String DEFAULT_SCOPE = "myscope";
    static final String DEFAULT_STREAM_NAME = "mystream";
    static final String DEFAULT_CONTROLLER_URI = "tls://localhost:9090";

    // Replace the value of the DEFAULT_TRUSTSTORE_PATH with "./pravega-client-examples/src/main/resources/cert.pem"
    // if you are running the tests directly from the IDE (as opposed to .
    static final String DEFAULT_TRUSTSTORE_PATH = "conf/cert.pem";

    static final String DEFAULT_USERNAME = "admin";
    static final String DEFAULT_PASSWORD = "1111_aaaa";
    static final String DEFAULT_ROUTING_KEY = "myroutingkey";
    static final String DEFAULT_MESSAGE = "hello secure world!";
    static final int NO_OF_SEGMENTS = 1;
}
