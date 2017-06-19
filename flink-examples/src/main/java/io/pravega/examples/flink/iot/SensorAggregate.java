package io.pravega.examples.flink.iot;

import org.apache.commons.lang3.StringUtils;

public class SensorAggregate {
    private long startTime;
    private int sensorId;
    private String location;
    private float tempMin;
    private float tempMax;

    public SensorAggregate(long startTime, int sensorId, String location, float tempMin, float tempMax) {
        this.startTime = startTime;
        this.sensorId = sensorId;
        this.location = location;
        this.tempMin = tempMin;
        this.tempMax = tempMax;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public float getTempMin() {
        return tempMin;
    }

    public void setTempMin(float tempMin) {
        this.tempMin = tempMin;
    }

    public float getTempMax() {
        return tempMax;
    }

    public void setTempMax(float tempMax) {
        this.tempMax = tempMax;
    }

    @Override
    public String toString() {
        return "SensorAggregate{" +
                "startTime=" + startTime +
                ", sensorId=" + sensorId +
                ", location='" + location + '\'' +
                ", tempMin=" + tempMin +
                ", tempMax=" + tempMax +
                '}';
    }
}
