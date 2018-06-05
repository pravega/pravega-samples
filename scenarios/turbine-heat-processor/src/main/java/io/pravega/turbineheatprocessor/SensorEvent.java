package io.pravega.turbineheatprocessor;

public class SensorEvent {
    private long timestamp;
    private int sensorId;
    private String location;
    private float temp;

    public SensorEvent() { }

    public SensorEvent(long timestamp, int sensorId, String location, float temp) {
        this.timestamp = timestamp;
        this.sensorId = sensorId;
        this.location = location;
        this.temp = temp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "timestamp=" + timestamp +
                ", sensorId=" + sensorId +
                ", location='" + location + '\'' +
                ", temp=" + temp +
                '}';
    }
}
