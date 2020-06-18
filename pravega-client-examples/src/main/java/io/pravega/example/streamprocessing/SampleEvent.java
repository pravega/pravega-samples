package io.pravega.example.streamprocessing;

public class SampleEvent {
    public long sequenceNumber;
    public String routingKey;
    public long intData;
    public long sum;
    public long timestamp;
    public String timestampStr;
    public long processedLatencyMs;
    public String processedBy;

    @Override
    public String toString() {
        return "SampleEvent{" +
                "sequenceNumber=" + String.format("%6d", sequenceNumber) +
                ", routingKey=" + routingKey +
                ", intData=" + String.format("%3d", intData) +
                ", sum=" + String.format("%8d", sum) +
                ", timestampStr=" + timestampStr +
                ", processedLatencyMs=" + String.format("%6d", processedLatencyMs) +
                ", processedBy=" + processedBy +
                '}';
    }
}
