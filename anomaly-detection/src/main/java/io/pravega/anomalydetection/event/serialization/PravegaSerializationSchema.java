/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.serialization;

import io.pravega.client.stream.Serializer;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A serialization schema adapter for a Pravega serializer.
 */
public class PravegaSerializationSchema<T extends Serializable> implements SerializationSchema<T> {
    private final Serializer<T> serializer;

    public PravegaSerializationSchema(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(T element) {
        ByteBuffer buf = serializer.serialize(element);
        assert buf.hasArray();
        return buf.array();
    }
}
