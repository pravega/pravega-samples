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
package io.pravega.examples.flink.alert;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {
    protected static final String STREAM_PARAM = "stream";
    protected static final String DEFAULT_STREAM = "myscope/apacheaccess";
    protected static final String CONTROLLER_PARAM = "controller";
    protected static final String DEFAULT_CONTROLLER = "tcp://127.0.0.1:9090";
    protected static final Integer ALERT_THRESHOLD = 6;
    protected static final Integer ALERT_WINDOW = 30;
    protected static final Integer ALERT_INTERVAL = 2;
}
