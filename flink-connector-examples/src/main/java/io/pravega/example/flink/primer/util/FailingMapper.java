/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.flink.primer.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.List;

/**
 * An identity mapper that throws an exception at a specified element.
 * The exception is only thrown during the first execution, prior to the first recovery.
 * <p>
 * <p>The function also fails, if the program terminates cleanly before the
 * function would throw an exception. That way, it guards against the case
 * where a failure is never triggered (for example because of a too high value for
 * the number of elements to pass before failing).
 */
public class FailingMapper<T> implements MapFunction<T, T>, CheckpointedFunction {

    /**
     * The number of elements to wait for, before failing
     */
    private final int failAtElement;

    private int elementCount;
    private boolean restored;

    /**
     * Creates a mapper that fails after processing the given number of elements.
     *
     * @param failAtElement The number of elements to wait for, before failing.
     */
    public FailingMapper(int failAtElement) {
        this.failAtElement = failAtElement;
    }

    @Override
    public T map(T element) throws Exception {
        if (!restored && ++elementCount > failAtElement) {
            System.out.println("Artificial failure at position " + elementCount);
            throw new IntentionalException("artificial failure");
        }

        return element;
    }

    @Override
    public String toString() {
        return "FAILING MAPPER: elementCount = " + elementCount + "," +
                "failedAtElement = " + failAtElement + "," +
                "restored = " + restored;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // do nothing
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (context.isRestored()) {
            restored = true;
        }
    }

    public static class IntentionalException extends Exception {
        public IntentionalException(String message) {
            super(message);
        }
    }
}
