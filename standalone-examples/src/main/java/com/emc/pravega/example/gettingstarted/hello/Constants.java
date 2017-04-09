/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.example.gettingstarted.hello;

/**
 * Defines a handful of constants shared by classes in this package.
 *
 */
public class Constants {
    protected static final String DEFAULT_SCOPE = "examples";
    protected static final String DEFAULT_STREAM_NAME = "helloStream";
    protected static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    
    protected static final String DEFAULT_ROUTING_KEY = "helloRoutingKey";
    protected static final String DEFAULT_MESSAGE = "hello world";
}
