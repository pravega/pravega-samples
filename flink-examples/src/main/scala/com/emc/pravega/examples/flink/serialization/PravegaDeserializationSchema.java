/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.examples.flink.serialization;
import com.emc.pravega.stream.Serializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A deserialization schema adapter for a Pravega serializer.
 */
public class PravegaDeserializationSchema<T extends Serializable> extends AbstractDeserializationSchema<T> {
    private final Class<T> typeClass;
    private final Serializer<T> serializer;

    public PravegaDeserializationSchema(Class<T> typeClass, Serializer<T> serializer) {
        this.typeClass = typeClass;
        this.serializer = serializer;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        ByteBuffer msg = ByteBuffer.wrap(message);
        return serializer.deserialize(msg);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(typeClass);
    }
}
