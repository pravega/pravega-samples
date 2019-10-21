package io.pravega.example.flink.watermark;

import java.io.Serializable;

public class SensorData implements Serializable {
    private int sensorId;
    private double value;
    private long timestamp;

    public SensorData() {
    }

    public SensorData(int sensorId, double value, long timestamp) {
        this.sensorId = sensorId;
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "sensorId=" + sensorId +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
