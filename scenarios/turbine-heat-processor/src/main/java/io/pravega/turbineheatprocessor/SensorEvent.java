/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
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
