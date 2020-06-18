/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.streamprocessing;

import java.net.URI;
import java.util.UUID;

/**
 * All parameters will come from environment variables.
 * This makes it easy to configure on Docker, Kubernetes, etc.
 */
class AppConfiguration {
    AppConfiguration(String[] args) {
    }

    // By default, we will connect to a standalone Pravega running on localhost.
    public URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER", "tcp://localhost:9090"));
    }

    public String getScope() {
        return getEnvVar("PRAVEGA_SCOPE", "examples");
    }

    public String getInstanceId() {
        return getEnvVar("INSTANCE_ID", UUID.randomUUID().toString());
    }

    public String getStream1Name() {
        return getEnvVar("PRAVEGA_STREAM_1", "streamprocessing1c");
    }

    public String getStream2Name() {
        return getEnvVar("PRAVEGA_STREAM_2", "streamprocessing2c");
    }

    public String getReaderGroup() {
        return getEnvVar("PRAVEGA_READER_GROUP", "streamprocessing1c-rg1");
    }

    public String getMembershipSynchronizerStreamName() {
        return getReaderGroup() + "-membership";
    }

    public int getTargetRateEventsPerSec() {
        return Integer.parseInt(getEnvVar("PRAVEGA_TARGET_RATE_EVENTS_PER_SEC", "10"));
    }

    public int getScaleFactor() {
        return Integer.parseInt(getEnvVar("PRAVEGA_SCALE_FACTOR", "2"));
    }

    public int getMinNumSegments() {
        return Integer.parseInt(getEnvVar("PRAVEGA_MIN_NUM_SEGMENTS", "6"));
    }

    public  long getCheckpointPeriodMs() {
        return Long.parseLong(getEnvVar("CHECKPOINT_PERIOD_MS", "3000"));
    }

    public long getCheckpointTimeoutMs() {
        return Long.parseLong(getEnvVar("CHECKPOINT_TIMEOUT_MS", "120000"));
    }

    public long getTransactionTimeoutMs() {
        return Long.parseLong(getEnvVar("TRANSACTION_TIMEOUT_MS", "120000"));
    }

    public long getHeartbeatIntervalMillis() {
        return Long.parseLong(getEnvVar("HEARTBEAT_INTERVAL_MS", "500"));
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
