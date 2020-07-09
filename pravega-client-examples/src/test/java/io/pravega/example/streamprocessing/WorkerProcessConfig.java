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

import io.pravega.client.ClientConfig;
import lombok.Builder;

@Builder
public class WorkerProcessConfig {
    public final String scope;
    public final ClientConfig clientConfig;
    public final String readerGroupName;
    public final String inputStreamName;
    public final String outputStreamName;
    public final String membershipSynchronizerStreamName;
    public final int numSegments;
    @Builder.Default public final long checkpointPeriodMs = 1000;
    @Builder.Default public final long heartbeatIntervalMillis = 1000;
    @Builder.Default public final long readTimeoutMillis = 1000;
}
