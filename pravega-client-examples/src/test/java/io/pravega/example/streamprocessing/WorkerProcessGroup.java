/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.streamprocessing;

import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * This manages a group of WorkerProcess instances.
 * The testing framework in {@link StreamProcessingTest} uses this class to
 * start and stop multiple instances of {@link AtLeastOnceProcessorInstrumented}
 * in a single process.
 */
@Builder
public class WorkerProcessGroup implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerProcessGroup.class);

    private final WorkerProcessConfig config;
    private final Map<Integer, WorkerProcess> workers = new HashMap<>();
    private WriteMode writeMode;

    public WorkerProcess get(int instanceId) {
        return workers.get(instanceId);
    }

    /**
     * Streams are guaranteed to exist after calling this method.
     */
    public void start(int... instanceIds) {
        IntStream.of(instanceIds).forEach(instanceId -> {
            log.info("start: instanceId={}", instanceId);
            workers.putIfAbsent(instanceId, WorkerProcess.builder().config(config).instanceId(instanceId).build());
        });
        IntStream.of(instanceIds).parallel().forEach(instanceId -> {
            final WorkerProcess worker = workers.get(instanceId);
            worker.init();
            worker.startAsync();
        });
    }

    /**
     * Processors are guaranteed to not process events after this method returns.
     */
    public void stop(int... instanceIds) {
        IntStream.of(instanceIds).forEach(instanceId -> {
            workers.get(instanceId).stopAsync();
        });
        IntStream.of(instanceIds).forEach(instanceId -> {
            try {
                workers.get(instanceId).awaitTerminated();
            } catch (Exception e) {
                log.warn("stop", e);
            }
            workers.remove(instanceId);
        });
    }

    protected int[] getInstanceIds() {
        return workers.keySet().stream().mapToInt(i -> i).toArray();
    }

    public void stopAll() {
        log.info("stopAll: workers={}", workers);
        stop(getInstanceIds());
    }

    @Override
    public void close() throws Exception {
        stopAll();
    }
}
