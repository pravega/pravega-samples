package io.pravega.example.streamprocessing;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.util.ReusableLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class AtLeastOnceProcessorInstrumented extends AtLeastOnceProcessor<TestEvent> {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceProcessorInstrumented.class);

    private final int instanceId;
    private final EventStreamWriter<TestEvent> writer;
    private final ReusableLatch latch;
    private final AtomicLong unflushedEventCount = new AtomicLong(0);
    private final AtomicBoolean preventFlushFlag = new AtomicBoolean(false);

    public AtLeastOnceProcessorInstrumented(
            Supplier<ReaderGroupPruner> pruner,
            Supplier<EventStreamReader<TestEvent>> reader,
            long readTimeoutMillis,
            int instanceId,
            EventStreamWriter<TestEvent> writer,
            ReusableLatch latch) {
        super(pruner, reader, readTimeoutMillis);
        this.instanceId = instanceId;
        this.writer = writer;
        this.latch = latch;
    }

    @Override
    public void process(EventRead<TestEvent> eventRead) {
        final TestEvent event = eventRead.getEvent();
        event.processedByInstanceId = instanceId;
        log.info("process: event={}", event);
        writer.writeEvent(Integer.toString(event.key), event);
        unflushedEventCount.incrementAndGet();
    }

    @Override
    public void flush() {
        if (preventFlushFlag.get()) {
            throw new RuntimeException("Flush called but this test requires that that flush not be called. Try to rerun the test.");
        }
        writer.flush();
        final long flushedEventCount = unflushedEventCount.getAndSet(0);
        log.info("flush: Flushed {} events", flushedEventCount);
    }

    @Override
    protected void injectFault(ReaderGroupPruner pruner) throws Exception {
        if (!latch.isReleased()) {
            log.warn("injectFault: BEGIN");
            // Pause pruner (but do not close it). This will also pause the membership synchronizer.
            pruner.pause();
            // Halt this processor thread until the latch is released.
            latch.await();
            pruner.unpause();
            log.warn("injectFault: END");
        }
    }

    public void preventFlush() {
        preventFlushFlag.set(true);
    }
}
