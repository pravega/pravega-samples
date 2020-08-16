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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * A special PravegaFixedSegmentsOutputFormat to store Terasort result into Pravega stream(s):
 *  each mapper/reducer task will write to a different stream, with the stream name as "OUTPUT_STREAM_PREFIX" + taskId;
 *  each stream is with a fixed one(1) segment.
 *
 * In the end, all the events are considered "in order" because:
 *  all the events in a stream are "in order" by their key (the first 10 bytes of event);
 *  any event of stream with lower taskId is smaller than any event of stream with greater taskId.
 *
 */
public class PravegaTeraSortOutputFormat<V> extends PravegaFixedSegmentsOutputFormat<V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaTeraSortOutputFormat.class);

    // Pravega stream name
    public static final String OUTPUT_STREAM_PREFIX = "pravega.output.stream.prefix";

    @Override
    public RecordWriter<String, V> getRecordWriter(TaskAttemptContext context) throws IOException {

        Configuration conf = context.getConfiguration();
        final String scopeName = Optional.ofNullable(conf.get(OUTPUT_SCOPE_NAME)).orElseThrow(() ->
                new IOException("The output scope name must be configured (" + OUTPUT_SCOPE_NAME + ")"));
        final String outputStreamPrefix = Optional.ofNullable(conf.get(OUTPUT_STREAM_PREFIX)).orElseThrow(() ->
                new IOException("The output stream prefix must be configured (" + OUTPUT_STREAM_PREFIX + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(OUTPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + OUTPUT_URI_STRING + ")"));
        final String deserializerClassName = Optional.ofNullable(conf.get(OUTPUT_DESERIALIZER)).orElseThrow(() ->
                new IOException("The event deserializer must be configured (" + OUTPUT_DESERIALIZER + ")"));

        final String outputStreamName = outputStreamPrefix + context.getTaskAttemptID().getTaskID().getId();

        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scopeName);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scopeName, outputStreamName, streamConfig);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,
                ClientConfig.builder().controllerURI(controllerURI).build());

        Serializer deserializer;
        try {
            Class<?> deserializerClass = Class.forName(deserializerClassName);
            deserializer = (Serializer<V>) deserializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating deserializer: {}", e);
            throw new IOException(
                    "Unable to create the event deserializer (" + deserializerClassName + ")", e);
        }

        EventStreamWriter<V> writer = clientFactory.createEventWriter(outputStreamName, deserializer,
                EventWriterConfig.builder().build());

        return new PravegaOutputRecordWriter<V>(writer);
    }

}
