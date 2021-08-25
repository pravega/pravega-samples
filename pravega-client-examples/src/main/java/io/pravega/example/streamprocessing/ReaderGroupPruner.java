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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.stream.ReaderGroup;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class removes unhealthy processes from a Pravega reader group.
 * It uses {@link MembershipSynchronizer} to identify healthy processes.
 */
@Slf4j
public class ReaderGroupPruner extends AbstractService implements AutoCloseable {
    private final ReaderGroup readerGroup;
    private final MembershipSynchronizer membershipSynchronizer;
    private final ScheduledExecutorService executor;
    private final long heartbeatIntervalMillis;

    private ScheduledFuture<?> task;

    public static ReaderGroupPruner create(ReaderGroup readerGroup, String membershipSynchronizerStreamName,
                                           String readerId, SynchronizerClientFactory clientFactory,
                                           ScheduledExecutorService executor, long heartbeatIntervalMillis) {
        final ReaderGroupPruner pruner = new ReaderGroupPruner(readerGroup, membershipSynchronizerStreamName, readerId, clientFactory,
                executor, heartbeatIntervalMillis);
        pruner.startAsync();
        pruner.awaitRunning();
        return pruner;
    }

    public ReaderGroupPruner(ReaderGroup readerGroup, String membershipSynchronizerStreamName, String readerId,
                             SynchronizerClientFactory clientFactory,
                             ScheduledExecutorService executor, long heartbeatIntervalMillis) {
        this.readerGroup = readerGroup;
        this.membershipSynchronizer = new MembershipSynchronizer(
                membershipSynchronizerStreamName,
                readerId,
                heartbeatIntervalMillis,
                clientFactory,
                executor,
                new MembershipSynchronizer.MembershipListener() {});
        this.executor = executor;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    private class PruneRunner implements Runnable {
        @Override
        public void run() {
            try {
                Set<String> rgMembers = readerGroup.getOnlineReaders();
                Set<String> msMembers = membershipSynchronizer.getCurrentMembers();
                log.debug("rgMembers={}", rgMembers);
                log.debug("msMembers={}", msMembers);
                rgMembers.removeAll(msMembers);
                rgMembers.forEach(readerId -> {
                    log.info("Removing dead reader {} from reader group {}", readerId, readerGroup.getGroupName());
                    readerGroup.readerOffline(readerId, null);
                });
            } catch (Exception e) {
                log.warn("Encountered an error while pruning reader group {}", readerGroup.getGroupName(), e);
                // Ignore error. It will retry at the next iteration.
            }
        }
    }

    @Override
    protected void doStart() {
        // We must ensure that we add this reader to the membership synchronizer before the reader group.
        membershipSynchronizer.startAsync();
        membershipSynchronizer.awaitRunning();
        // Initial delay will be between 50% and 100% of the heartbeat interval.
        // Although this is not needed for correctness, it reduces unnecessary load
        // caused by multiple processes attempting to put the same reader offline.
        final long initialDelay = (long) (heartbeatIntervalMillis * (0.5 + Math.random() * 0.5));
        // Period will between 90% and 100% of the heartbeat interval.
        final long period = (long) (heartbeatIntervalMillis * (0.9 + Math.random() * 0.1));
        task = executor.scheduleAtFixedRate(new PruneRunner(), initialDelay, period, TimeUnit.MILLISECONDS);
        notifyStarted();
    }

    @Override
    protected void doStop() {
        task.cancel(false);
        membershipSynchronizer.stopAsync();
    }

    @VisibleForTesting
    public void pause() {
        log.warn("paused");
        task.cancel(false);
        membershipSynchronizer.pause();
    }

    /**
     * Not implemented.
     */
    @VisibleForTesting
    public void unpause() {
        log.warn("unpause");
        membershipSynchronizer.unpause();
    }

    @Override
    public void close() throws Exception {
        stopAsync();
    }
}
