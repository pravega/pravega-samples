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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OutputFormat that can be added as a storage to write events to Pravega.
 */
public class PravegaOutputFormat<V> extends OutputFormat<String, V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaOutputFormat.class);

    // Pravega scope name
    public static final String SCOPE_NAME = "pravega.scope";
    // Pravega stream name
    public static final String STREAM_NAME = "pravega.stream";
    // Pravega uri string
    public static final String URI_STRING = "pravega.uri";
    // Pravega deserializer class name
    public static final String DESERIALIZER = "pravega.deserializer";

    private static final long DEFAULT_TXN_TIMEOUT_MS = 30000L;
    private static final long DEFAULT_TXN_MAX_EXECUTION_TIME_MS = 30000L;
    private static final long DEFAULT_TXN_SCALE_GRACE_PERIOD_MS = 30000L;
    private static final long DEFAULT_PING_LEASE_MS = 30000L;

    // client factory
    private ClientFactory externalClientFactory;

    public PravegaOutputFormat() {
    }

    @VisibleForTesting
    protected PravegaOutputFormat(ClientFactory externalClientFactory) {
        this.externalClientFactory = externalClientFactory;
    }

    @Override
    public RecordWriter<String, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        final String scopeName = Optional.ofNullable(conf.get(PravegaOutputFormat.SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaOutputFormat.SCOPE_NAME + ")"));
        final String streamName = Optional.ofNullable(conf.get(PravegaOutputFormat.STREAM_NAME)).orElseThrow(() ->
                new IOException("The input stream name must be configured (" + PravegaOutputFormat.STREAM_NAME + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(PravegaOutputFormat.URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaOutputFormat.URI_STRING + ")"));
        final String deserializerClassName = Optional.ofNullable(conf.get(PravegaOutputFormat.DESERIALIZER)).orElseThrow(() ->
                new IOException("The event deserializer must be configured (" + PravegaOutputFormat.DESERIALIZER + ")"));

        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scopeName);

        StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scopeName).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build();

        streamManager.createStream(scopeName, streamName, streamConfig);
        ClientFactory clientFactory = (externalClientFactory != null) ? externalClientFactory : ClientFactory.withScope(scopeName, controllerURI);

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
                .transactionTimeoutTime(DEFAULT_TXN_TIMEOUT_MS)
                .transactionTimeoutScaleGracePeriod(DEFAULT_TXN_SCALE_GRACE_PERIOD_MS)
                .build());

        return new PravegaOutputRecordWriter<V>(writer);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        // tmp solution, not for production
        return new FileOutputCommitter(new Path("/tmp/" + context.getTaskAttemptID().getJobID().toString()), context);
    }
}
