/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.hadoop.wordcount;

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

    public PravegaOutputRecordWriter(EventStreamWriter writer) {
        this.writer = writer;
    }

    @Override
    public void write(String key, V value) throws IOException, InterruptedException {
        final CompletableFuture<Void> future = writer.writeEvent(key, value);
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
