/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flink.streamcuts.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.pravega.shaded.com.google.common.collect.ImmutableMap;
import io.pravega.shaded.com.google.common.collect.ImmutableTable;
import io.pravega.shaded.com.google.common.collect.Maps;
import org.apache.flink.api.common.ExecutionConfig;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for handling Guava ImmutableMap used in StreamCutImpl
 */
public class GuavaImmutableMapSerializer extends Serializer<ImmutableMap<Object, ? extends Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public GuavaImmutableMapSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableMap<Object, ? extends Object> immutableMap) {
        kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public ImmutableMap<Object, Object> read(Kryo kryo, Input input, Class<? extends ImmutableMap<Object, ? extends Object>> type) {
        Map map = kryo.readObject(input, HashMap.class);
        return ImmutableMap.copyOf(map);
    }

    private enum DummyEnum {
        VALUE1,
        VALUE2
    }

    public static void registerSerializers(ExecutionConfig conf) {

        Class<GuavaImmutableMapSerializer> serializer = GuavaImmutableMapSerializer.class;

        conf.registerTypeWithKryoSerializer(ImmutableMap.class, serializer);
        conf.registerTypeWithKryoSerializer(ImmutableMap.of().getClass(), serializer);
        Object o1 = new Object();
        Object o2 = new Object();
        conf.registerTypeWithKryoSerializer(ImmutableMap.of(o1, o1).getClass(), serializer);
        conf.registerTypeWithKryoSerializer(ImmutableMap.of(o1, o1, o2, o2).getClass(), serializer);
        Map<DummyEnum, Object> enumMap = new EnumMap<>(DummyEnum.class);
        for (DummyEnum e : DummyEnum.values()) {
            enumMap.put(e, o1);
        }
        conf.registerTypeWithKryoSerializer(ImmutableMap.copyOf(enumMap).getClass(), serializer);
        ImmutableTable<Object, Object, Object> denseImmutableTable = ImmutableTable.builder().put("a", 1, 1).put("b", 1, 1).build();
        conf.registerTypeWithKryoSerializer(denseImmutableTable.rowMap().getClass(), serializer);
        conf.registerTypeWithKryoSerializer(((Map) denseImmutableTable.rowMap().get("a")).getClass(), serializer);
        conf.registerTypeWithKryoSerializer(denseImmutableTable.columnMap().getClass(), serializer);
        conf.registerTypeWithKryoSerializer(((Map) denseImmutableTable.columnMap().get(1)).getClass(), serializer);
    }
}
