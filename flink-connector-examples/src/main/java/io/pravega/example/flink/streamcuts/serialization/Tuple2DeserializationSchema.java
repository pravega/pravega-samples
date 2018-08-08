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
package io.pravega.example.flink.streamcuts.serialization;

import io.pravega.client.stream.impl.JavaSerializer;
import java.nio.ByteBuffer;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple2DeserializationSchema extends AbstractDeserializationSchema<Tuple2<Integer, Double>> {

    private JavaSerializer<Tuple2<Integer, Double>> serializer = new JavaSerializer<>();

    @Override
    public Tuple2<Integer, Double> deserialize(byte[] message) {
        return serializer.deserialize(ByteBuffer.wrap(message));
    }

    @Override
    public boolean isEndOfStream(Tuple2<Integer, Double> nextElement) {
        return false;
    }
}