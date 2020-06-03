package io.pravega.example.streamprocessing;

import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.stream.ReaderGroup;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ReaderGroupPruner extends AbstractService implements AutoCloseable {
    private final ReaderGroup readerGroup;
    private final MembershipSynchronizer membershipSynchronizer;
    private final ScheduledExecutorService executor;
    private final long heartbeatIntervalMillis;

    private ScheduledFuture<?> task;

    public static ReaderGroupPruner create(ReaderGroup readerGroup, String membershipSynchronizerStreamName, String readerId, SynchronizerClientFactory clientFactory, ScheduledExecutorService executor, long heartbeatIntervalMillis) {
        final ReaderGroupPruner pruner = new ReaderGroupPruner(readerGroup, membershipSynchronizerStreamName, readerId, clientFactory,
                executor, heartbeatIntervalMillis);
        pruner.startAsync();
        pruner.awaitRunning();
        return pruner;
    }

    public ReaderGroupPruner(ReaderGroup readerGroup, String membershipSynchronizerStreamName, String readerId, SynchronizerClientFactory clientFactory,
                             ScheduledExecutorService executor, long heartbeatIntervalMillis) {
        this.readerGroup = readerGroup;

        // No-op listener
        final MembershipSynchronizer.MembershipListener membershipListener = new MembershipSynchronizer.MembershipListener() {
            @Override
            public void healthy() {
            }

            @Override
            public void unhealthy() {
            }
        };
        this.membershipSynchronizer = new MembershipSynchronizer(membershipSynchronizerStreamName, readerId, clientFactory, executor, membershipListener);
        this.executor = executor;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    private class PruneRunner implements Runnable {
        @Override
        public void run() {
            try {
                Set<String> rgMembers = readerGroup.getOnlineReaders();
                Set<String> msMembers = membershipSynchronizer.getCurrentMembers();
                rgMembers.removeAll(msMembers);
                rgMembers.forEach(readerId -> readerGroup.readerOffline(readerId, null));
            } catch (Exception e) {
                log.warn("Encountered an error while pruning reader group {}: ", readerGroup.getGroupName(), e);
                // Ignore error. It will retry at the next iteration.
            }
        }
    }

    @Override
    protected void doStart() {
        // Must ensure that we add this reader to MS before RG.
        membershipSynchronizer.startAsync();
        membershipSynchronizer.awaitRunning();
        task = executor.scheduleAtFixedRate(new PruneRunner(), heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        notifyStarted();
    }

    @Override
    protected void doStop() {
        task.cancel(false);
        membershipSynchronizer.stopAsync();
        // TODO: Can we safely delete the membershipSynchronizer stream?
    }

    @Override
    public void close() throws Exception {
        stopAsync();
    }
}
