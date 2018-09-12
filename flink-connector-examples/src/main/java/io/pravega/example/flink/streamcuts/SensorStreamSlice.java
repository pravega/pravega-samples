/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flink.streamcuts;

import io.pravega.client.stream.StreamCut;
import java.io.Serializable;

/**
 * Class that contains a pair of StreamCut objects representing a slice of a stream for a single sensor.
 */
public class SensorStreamSlice implements Serializable {

    private StreamCut start;
    private StreamCut end;
    private int sensorId;

    public SensorStreamSlice(int sensorId) {
        this.sensorId = sensorId;
    }

    public StreamCut getStart() {
        return start;
    }

    public StreamCut getEnd() {
        return end;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setStart(StreamCut start) {
        this.start = start;
    }

    public void setEnd(StreamCut end) {
        this.end = end;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    @Override
    public String toString() {
        return "Start StreamCut: " + ((start != null) ? start.toString() : " null ") +
               ", end StreamCut: " + ((end != null) ? end.toString() : "null ") +
               ", sensorId: " + sensorId;
    }
}
