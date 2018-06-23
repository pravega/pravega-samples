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
