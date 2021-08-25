/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.example.streamprocessing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.pravega.client.stream.Serializer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implmentation of {@link Serializer} that uses JSON serialization.
 * @param <T> Type of Object to be serialized.
 */
@Slf4j
@AllArgsConstructor
public class JSONSerializer<T> implements Serializer<T> {

    /**
     * This is used to represent the generic type. The user can use {@link com.google.gson.reflect.TypeToken} to represent
     * the type of the object which will be serialized/deserialized.
     */
    private final Type type;
    private final Gson gson = new GsonBuilder().create();

    /**
     * {@inheritDoc}
     * JSON based serialization.
     */
    @Override
    public ByteBuffer serialize(T value) {
        return ByteBuffer.wrap(gson.toJson(value, type).getBytes(UTF_8));
    }

    /**
     * {@inheritDoc}
     * JSON based deserialization.
     */
    @Override
    public T deserialize(ByteBuffer serializedValue) {
        return gson.fromJson(UTF_8.decode(serializedValue).toString(), type);
    }

}