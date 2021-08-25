package io.pravega.example.streamprocessing;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.util.ReusableLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * An AtLeastOnceProcessor that is instrumented for testing.
 */
public class AtLeastOnceProcessorInstrumented extends AtLeastOnceProcessor<TestEvent> {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceProcessorInstrumented.class);

    private final int instanceId;
    private final EventStreamWriter<TestEvent> writer;

    private final ReusableLatch pauseLatch = new ReusableLatch(true);
    private final AtomicLong unflushedEventCount = new AtomicLong(0);
    private final AtomicBoolean induceFailureDuringFlushFlag = new AtomicBoolean(false);
    private final AtomicBoolean induceFailureDuringProcessFlag = new AtomicBoolean(false);
    private final AtomicReference<WriteMode> writeModeRef = new AtomicReference<>();
    // A queue containing unflushed events.
    private final List<TestEvent> queue = new ArrayList<>();

    public AtLeastOnceProcessorInstrumented(
            Supplier<ReaderGroupPruner> pruner,
            Supplier<EventStreamReader<TestEvent>> reader,
            long readTimeoutMillis,
            WriteMode writeMode,
            int instanceId,
            EventStreamWriter<TestEvent> writer) {
        super(pruner, reader, readTimeoutMillis);
        writeModeRef.set(writeMode);
        this.instanceId = instanceId;
        this.writer = writer;
    }

    @Override
    public void process(EventRead<TestEvent> eventRead) throws Exception {
        final TestEvent event = eventRead.getEvent();
        event.processedByInstanceId = instanceId;
        final WriteMode writeMode = writeModeRef.get();
        log.info("process: writeMode={}, event={}", writeMode, event);
        if (induceFailureDuringProcessFlag.get()) {
            throw new RuntimeException("induceFailureDuringProcess is set");
        }
        if (writeMode == WriteMode.AlwaysHoldUntilFlushed) {
            queue.add(event);
            unflushedEventCount.incrementAndGet();
        } else {
            final CompletableFuture<Void> future = writer.writeEvent(Integer.toString(event.key), event);
            if (writeMode == WriteMode.AlwaysDurable) {
                future.get();
            } else {
                unflushedEventCount.incrementAndGet();
            }
        }
    }

    @Override
    public void flush() {
        if (induceFailureDuringFlushFlag.get()) {
            log.warn("induceFailureDuringFlushFlag is set");
            throw new RuntimeException("induceFailureDuringFlushFlag is set");
        }
        log.info("flush: Writing {} queued events", queue.size());
        queue.forEach((event) -> writer.writeEvent(Integer.toString(event.key), event));
        queue.clear();
        writer.flush();
        final long flushedEventCount = unflushedEventCount.getAndSet(0);
        log.info("flush: Flushed {} events", flushedEventCount);
    }

    @Override
    protected void injectFault(ReaderGroupPruner pruner) throws Exception {
        if (!pauseLatch.isReleased()) {
            log.warn("injectFault: BEGIN");
            // Pause pruner (but do not close it). This will also pause the membership synchronizer.
            pruner.pause();
            // Halt this processor thread until the latch is released.
            pauseLatch.await();
            pruner.unpause();
            log.warn("injectFault: END");
        }
    }

    public void pause() {
        pauseLatch.reset();
    }

    public void unpause() {
        pauseLatch.release();
    }

    public void induceFailureDuringProcess() {
        induceFailureDuringProcessFlag.set(true);
    }

    public void induceFailureDuringFlush() {
        induceFailureDuringFlushFlag.set(true);
    }

    public void setWriteMode(WriteMode writeMode) {
        writeModeRef.set(writeMode);
    }
}
