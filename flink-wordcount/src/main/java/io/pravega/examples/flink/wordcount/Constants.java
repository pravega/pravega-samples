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
package io.pravega.examples.flink.wordcount;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {
    protected static final String HOST_PARAM = "host";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final String PORT_PARAM = "port";
    protected static final String DEFAULT_PORT = "port";
    protected static final String STREAM_PARAM = "stream";
    protected static final String DEFAULT_STREAM = "myscope/wordcount";
    protected static final String CONTROLLER_PARAM = "controller";
    protected static final String DEFAULT_CONTROLLER = "tcp://127.0.0.1:9090";
    protected static final String WORD_SEPARATOR = " ";
}
