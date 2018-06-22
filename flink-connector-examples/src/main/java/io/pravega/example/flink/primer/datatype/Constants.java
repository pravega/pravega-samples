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
package io.pravega.example.flink.primer.datatype;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {
    public static final String SCOPE_PARAM = "scope";
    public static final String DEFAULT_SCOPE = "examples";
    public static final String STREAM_PARAM = "stream";
    public static final String DEFAULT_STREAM = "mystream";
    public static final String Default_URI_PARAM = "controller";
    public static final String Default_URI = "tcp://localhost:9090";
    public static final String USERNAME_PARAM = "username";
    public static final String PASSWORD_PARAM = "password";
    public static final Integer ALERT_THRESHOLD = 6;
    public static final Integer ALERT_WINDOW = 30;
    public static final Integer ALERT_INTERVAL = 2;
}
