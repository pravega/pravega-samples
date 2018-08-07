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
 * Class that contains a pair of StreamCut objects representing a slice of a stream.
 */
public class StreamSlice implements Serializable {

    private StreamCut start;
    private StreamCut end;

    public StreamSlice (StreamCut start, StreamCut end) {
        this.start = start;
        this.end = end;
    }

    public StreamCut getStart() {
        return start;
    }

    public StreamCut getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "Start StreamCut: " + ((start != null) ? start.toString() : "") +
               ", end StreamCut: " + ((end != null) ? end.toString() : "");
    }
}
