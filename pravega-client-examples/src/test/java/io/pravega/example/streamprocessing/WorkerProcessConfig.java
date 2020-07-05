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
