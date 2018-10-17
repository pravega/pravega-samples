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
public class PravegaGlobalOrderOutputFormat<V> extends PravegaOutputFormat<V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaGlobalOrderOutputFormat.class);

    public static final String OUT_STREAM_PREFIX = "pravega.out.stream.prefix";

    @Override
    public RecordWriter<String, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        final String scopeName = Optional.ofNullable(conf.get(PravegaGlobalOrderOutputFormat.SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaGlobalOrderOutputFormat.SCOPE_NAME + ")"));
        final String streamPrefix = Optional.ofNullable(conf.get(PravegaGlobalOrderOutputFormat.OUT_STREAM_PREFIX)).orElseThrow(() ->
                new IOException("The output stream prefix must be configured (" + PravegaGlobalOrderOutputFormat.OUT_STREAM_PREFIX + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(PravegaGlobalOrderOutputFormat.URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaGlobalOrderOutputFormat.URI_STRING + ")"));
        final String deserializerClassName = Optional.ofNullable(conf.get(PravegaGlobalOrderOutputFormat.DESERIALIZER)).orElseThrow(() ->
                new IOException("The event deserializer must be configured (" + PravegaGlobalOrderOutputFormat.DESERIALIZER + ")"));

        final String streamName = streamPrefix + context.getTaskAttemptID().getTaskID().getId();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scopeName);

        StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scopeName).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scopeName, streamName, streamConfig);
        ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI);

        Serializer deserializer;
        try {
            Class<?> deserializerClass = Class.forName(deserializerClassName);
            deserializer = (Serializer<V>) deserializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating deserializer: {}", e);
            throw new IOException(
                    "Unable to create the event deserializer (" + deserializerClassName + ")", e);
        }

        EventStreamWriter<V> writer = clientFactory.createEventWriter(streamName, deserializer, EventWriterConfig.builder()
                .build());

        return new PravegaOutputRecordWriter<V>(writer);
    }

}
