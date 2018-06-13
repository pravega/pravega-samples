/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.hadoop;

import io.pravega.client.stream.Serializer;
import lombok.EqualsAndHashCode;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * An implementation of {@link Serializer} that uses serialization.
 */
@EqualsAndHashCode
public class TextSerializer implements Serializer<Text>, Serializable {

    @Override
    public ByteBuffer serialize(Text value) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout;
        try {
            oout = new ObjectOutputStream(bout);
            value.write(oout);
            oout.close();
            bout.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(bout.toByteArray());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Text deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        ObjectInputStream oin;
        Text value = new Text();
        try {
            oin = new ObjectInputStream(bin);
            value.readFields(oin);
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
