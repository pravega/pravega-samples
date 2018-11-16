/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.pravega.example.hadoop;

import io.pravega.client.stream.EventStreamWriter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A RecordWriter that can write events to Pravega.
 */
@NotThreadSafe
public class PravegaOutputRecordWriter<V> extends RecordWriter<String, V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaOutputRecordWriter.class);
    private final EventStreamWriter writer;
    private final String routingKey;

    public PravegaOutputRecordWriter(EventStreamWriter writer, String routingKey) {
        this.writer = writer;
        this.routingKey = routingKey;
    }

    @Override
    public void write(String key, V value) throws IOException, InterruptedException {
        final CompletableFuture<Void> future = routingKey == null ?
                writer.writeEvent(key, value) : writer.writeEvent(routingKey, value);
        future.whenCompleteAsync(
            (v, e) -> {
                if (e != null) {
                    log.warn("Detected a write failure: {}", e);
                }
            }
        );
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        writer.close();
    }
}
