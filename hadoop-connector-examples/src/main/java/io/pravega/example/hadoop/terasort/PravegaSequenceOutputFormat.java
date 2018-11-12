/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.hadoop.terasort;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.example.hadoop.wordcount.PravegaOutputFormat;
import io.pravega.example.hadoop.wordcount.PravegaOutputRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * A special PravegaOutputFormat to write events to Pravega in global order, which means:
 *   For each reducer (or maper if no reducer) the job creates a separate Pravega stream;
 *   There is only one segment for the stream;
 *   Events are ordered inside a stream;
 *   Events are globally orders across all the output streams.
 *
 * (This is to mimic how Hadoop Terasort generates one file per reducer after sorting)
 */
public class PravegaSequenceOutputFormat<V> extends PravegaOutputFormat<V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaSequenceOutputFormat.class);

    @Override
    public RecordWriter<String, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return getRecordWriter(context, String.valueOf(context.getTaskAttemptID().getTaskID().getId()));
    }

}
