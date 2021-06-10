/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.pravega.example.flink.primer.source;


import io.pravega.example.flink.primer.datatype.IntegerEvent;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ThrottledIntegerEventProducer
        extends RichParallelSourceFunction<IntegerEvent>
        implements ListCheckpointed<Long>, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(ThrottledIntegerEventProducer.class);

    /**
     * Blocker when the generator needs to wait for the checkpoint to happen.
     * Eager initialization means it must be serializable (pick any serializable type)
     */
    private final Object blocker = new SerializableObject();

    // The total number of events to generate
    private final int numEventsTotal;

    // The position to have at least one checkpoint
    private final int latestPosForCheckpoint;

    // The current position in the sequence of numbers
    private long currentPosition = -1;

    // The last checkpoint triggered;
    private long lastCheckpointTriggered;

    // The last checkpoint confirmed.
    private long lastCheckpointConfirmed;

    // The position of last checkpoint
    private long checkpointPosition = -1;

    // Set to true after restore
    private boolean restored = false;

    //Flag to cancel the source. Must be volatile, because modified asynchronously
    private volatile boolean running = true;

    public ThrottledIntegerEventProducer(final int numEventsTotal)
    {
        this(numEventsTotal, numEventsTotal / 3);
    }

    public ThrottledIntegerEventProducer(final int numEventsTotal, final int latestPosForCheckpoint) {
        Preconditions.checkArgument(numEventsTotal > 0);
        Preconditions.checkArgument(latestPosForCheckpoint >= 0 && latestPosForCheckpoint < numEventsTotal);

        this.numEventsTotal = numEventsTotal;
        this.latestPosForCheckpoint = latestPosForCheckpoint;
    }

    @Override
    public String toString() {
        return "numEventsTotal = " + numEventsTotal + "," +
                "latestPosForCheckpoint = " + latestPosForCheckpoint + "," +
                "snapshotPosition = " + checkpointPosition + "," +
                "currentPosition = " + currentPosition + "," +
                "lastCheckpointTriggered = " + lastCheckpointTriggered + "," +
                "lastCheckpointConfirmed = " + lastCheckpointConfirmed;
    }

    /**
     * @param ctx The context to emit elements to and for accessing locks.
     */
    @Override
    public void run(SourceContext<IntegerEvent> ctx) throws Exception {


        // each source subtask emits only the numbers where (num % parallelism == subtask_index)
        final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
        long current = this.currentPosition >= 0 ? this.currentPosition : getRuntimeContext().getIndexOfThisSubtask();

        synchronized (ctx.getCheckpointLock()) {
            if (!restored) {
                // beginning of stream
                ctx.collect(new IntegerEvent(IntegerEvent.start));
            }
        }

        while (this.running && current < this.numEventsTotal) {

            // throttle if no checkpoint happened so far
            if (this.lastCheckpointConfirmed < 1) {
                if (current < this.latestPosForCheckpoint) {
                    Thread.sleep(5);
                } else {
                    synchronized (blocker) {
                        while (this.running && this.lastCheckpointConfirmed < 1 ) {
                            blocker.wait();
                        }
                    }
                }
            }

            // emit the next event
            current += stepSize;
            synchronized (ctx.getCheckpointLock()) {
                //System.out.println("Emitting: current = " + current + ", " + toString());
                ctx.collect(new IntegerEvent(current));
                this.currentPosition = current;
            }
        }

        synchronized (ctx.getCheckpointLock()) {
            if (restored) {
                // end of stream
                ctx.collect(new IntegerEvent(IntegerEvent.end));
            }
        }

        // after we are done, we need to wait for two more checkpoint to complete
        // before finishing the program - that is to be on the safe side that
        // the sink also got the "commit" notification for all relevant checkpoints
        // and committed the data to pravega

        // note: this indicates that to handle finite jobs with 2PC outputs more
        // easily, we need a primitive like "finish-with-checkpoint" in Flink

        final long lastCheckpoint;
        synchronized (ctx.getCheckpointLock()) {
            lastCheckpoint = this.lastCheckpointTriggered;
        }

        synchronized (this.blocker) {
            while (this.lastCheckpointConfirmed <= lastCheckpoint + 1) {
                this.blocker.wait();
            }
        }
    }

    /**
     * Cancels the source. Most sources will have a while loop inside the
     * {@link #run(SourceContext)} method. The implementation needs to ensure that the
     * source will break out of that loop after this method is called.
     * <p>
     */
    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        this.lastCheckpointTriggered = checkpointId;
        this.checkpointPosition = this.currentPosition;
        System.out.println("Start checkpointing at position " + this.currentPosition);
        return Collections.singletonList(this.currentPosition);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        this.currentPosition = state.get(0);

        // at least one checkpoint must have happened so far
        this.lastCheckpointTriggered = 1L;
        this.lastCheckpointConfirmed = 1L;
        this.restored = true;
        System.out.println("Restore from checkpoint at position " + this.currentPosition);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

        synchronized (blocker) {
            if (checkpointPosition > -1) {
                // confirm only after a "real" checkpoint, i.e., position is greater than -1.
                this.lastCheckpointConfirmed = checkpointId;
                System.out.println("Complete checkpointing at position " + this.checkpointPosition);
            }
            blocker.notifyAll();
        }
    }

}