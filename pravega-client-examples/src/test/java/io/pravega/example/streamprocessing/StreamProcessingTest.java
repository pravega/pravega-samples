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

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.utils.EventStreamReaderIterator;
import io.pravega.utils.SetupUtils;
import lombok.Builder;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamProcessingTest {
    static final Logger log = LoggerFactory.getLogger(StreamProcessingTest.class);

    protected static final AtomicReference<SetupUtils> SETUP_UTILS = new AtomicReference<>();

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.set(new SetupUtils());
        SETUP_UTILS.get().startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @RequiredArgsConstructor
    static class TestContext {
        final EventStreamWriter<TestEvent> writer;
        final EventStreamReaderIterator<TestEvent> readerIterator;
        final TestEventGenerator generator;
        final TestEventValidator validator;
        final WorkerProcessGroup workerProcessGroup;
        final long checkpointPeriodMs;
    }

    /**
     * Write the given number of events to the Pravega input stream.
     *
     * @param ctx provides access to the generator, writer, etc.
     * @param numEvents number of events to write
     */
    void writeEvents(TestContext ctx, int numEvents) {
        Iterators.limit(ctx.generator, numEvents).forEachRemaining(event -> ctx.writer.writeEvent(Integer.toString(event.key), event));
    }

    /**
     * Read events from the Pravega output stream to ensure that the processors produced the correct result.
     *
     * @param ctx provides access to the validator, reader, etc.
     * @param expectedInstanceIds All read events must have a processedByInstanceId in this set.
     */
    void validateEvents(TestContext ctx, int[] expectedInstanceIds) {
        log.info("validateEvents: BEGIN");
        // Read events from output stream. Return when complete or throw exception if out of order or timeout.
        ctx.validator.clearCounters();
        ctx.validator.validate(ctx.readerIterator, ctx.generator.getLastSequenceNumbers());
        // Confirm that only instances in expectedInstanceIds have processed the events.
        final Map<Integer, Long> eventCountByInstanceId = ctx.validator.getEventCountByInstanceId();
        final Set<Integer> actualInstanceIds = eventCountByInstanceId.keySet();
        final Set<Integer> expectedInstanceIdsSet = Arrays.stream(expectedInstanceIds).boxed().collect(Collectors.toCollection(HashSet::new));
        log.info("validateEvents: eventCountByInstanceId={}, expectedInstanceIdsSet={}", eventCountByInstanceId, expectedInstanceIdsSet);
        Assert.assertTrue(MessageFormat.format("eventCountByInstanceId={0}, expectedInstanceIdsSet={1}", eventCountByInstanceId, expectedInstanceIdsSet),
                Sets.difference(actualInstanceIds, expectedInstanceIdsSet).isEmpty());
        // Warn if any instances are idle. This cannot be an assertion because this may happen under normal conditions.
        final Sets.SetView<Integer> idleInstanceIds = Sets.difference(expectedInstanceIdsSet, actualInstanceIds);
        if (!idleInstanceIds.isEmpty()) {
            log.warn("validateEvents: Some instances processed no events; eventCountByInstanceId={}, expectedInstanceIdsSet={}",
                    eventCountByInstanceId, expectedInstanceIdsSet);
        }
        log.info("validateEvents: END");
    }

    /**
     * Write the given number of events to the Pravega input stream.
     * Then read events from the Pravega output stream to ensure that the processors produced the correct result.
     *
     * @param ctx provides access to the generator, writer, etc.
     * @param numEvents number of events to write
     * @param expectedInstanceIds All read events must have a processedByInstanceId in this set.
     */
    void writeEventsAndValidate(TestContext ctx, int numEvents, int[] expectedInstanceIds) {
        writeEvents(ctx, numEvents);
        validateEvents(ctx, expectedInstanceIds);
    }

    /**
     * Write events to a Pravega stream, read events from the same stream, and validate expected ordering.
     */
    @Test
    public void noProcessorTest() throws Exception {
        final String methodName = (new Object() {
        }).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        // Create stream.
        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        final String inputStreamName = "stream-" + UUID.randomUUID().toString();
        SETUP_UTILS.get().createTestStream(inputStreamName, 6);
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Prepare writer that will write to the stream.
        final Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>() {}.getType());
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        @Cleanup
        final EventStreamWriter<TestEvent> writer = clientFactory.createEventWriter(inputStreamName, serializer, eventWriterConfig);

        // Prepare reader that will read from the stream.
        final String outputStreamName = inputStreamName;
        final String readerGroup = "rg" + UUID.randomUUID().toString().replace("-", "");
        final String readerId = "reader-" + UUID.randomUUID().toString();
        final ReaderConfig readerConfig = ReaderConfig.builder().build();
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(SETUP_UTILS.get().getStream(outputStreamName))
                .build();
        @Cleanup
        final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        @Cleanup
        final EventStreamReader<TestEvent> reader = clientFactory.createReader(
                readerId,
                readerGroup,
                new JSONSerializer<>(new TypeToken<TestEvent>() {}.getType()),
                readerConfig);
        EventStreamReaderIterator<TestEvent> readerIterator = new EventStreamReaderIterator<>(reader, 30000);

        final TestEventGenerator generator = new TestEventGenerator(6);
        final TestEventValidator validator = new TestEventValidator();
        final TestContext ctx = new TestContext(writer, readerIterator, generator, validator, null, 0);

        writeEventsAndValidate(ctx, 13, new int[]{-1});
        writeEventsAndValidate(ctx, 3, new int[]{-1});
        writeEventsAndValidate(ctx, 15, new int[]{-1});
    }

    @Builder
    protected static class EndToEndTestConfig {
        // number of stream segments
        @Builder.Default
        public final int numSegments = 1;
        // number of unique routi
        @Builder.Default
        public final int numKeys = 1;
        // number of initial processor instances
        @Builder.Default
        public final int numInitialInstances = 1;
        @Builder.Default
        public final WriteMode writeMode = WriteMode.Default;
        // test-specific function to write and validate events, etc.
        @Builder.Default
        public final Consumer<TestContext> func = (ctx) -> {};
    }

    /**
     * Write events to the input stream. Start multiple processors which can read from the input stream
     * and write to the output stream. Validate events in the output stream.
     * This method performs the setup and teardown. The provided function func performs the actual write and read
     * of events; stops, pauses, and starts processor instances; and validates results.
     */
    private void run(EndToEndTestConfig config) throws Exception {
        final String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        final String scope = SETUP_UTILS.get().getScope();
        final ClientConfig clientConfig = SETUP_UTILS.get().getClientConfig();
        final String inputStreamName = "input-stream-" + UUID.randomUUID().toString();
        final String outputStreamName = "output-stream-" + UUID.randomUUID().toString();
        final String membershipSynchronizerStreamName = "ms-" + UUID.randomUUID().toString();
        final String inputStreamReaderGroupName = "rg" + UUID.randomUUID().toString().replace("-", "");
        final long checkpointPeriodMs = 1000;

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        final WorkerProcessConfig workerProcessConfig = WorkerProcessConfig.builder()
                .scope(scope)
                .clientConfig(clientConfig)
                .readerGroupName(inputStreamReaderGroupName)
                .inputStreamName(inputStreamName)
                .outputStreamName(outputStreamName)
                .membershipSynchronizerStreamName(membershipSynchronizerStreamName)
                .numSegments(config.numSegments)
                .checkpointPeriodMs(checkpointPeriodMs)
                .writeMode(config.writeMode)
                .build();
        @Cleanup
        final WorkerProcessGroup workerProcessGroup = WorkerProcessGroup.builder().config(workerProcessConfig).build();

        // Start initial set of processors. This will also create the necessary streams.
        workerProcessGroup.start(IntStream.range(0, config.numInitialInstances).toArray());

        // Prepare generator writer that will write to the stream read by the processor.
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        final Serializer<TestEvent> serializer = new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType());
        final EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        @Cleanup
        final EventStreamWriter<TestEvent> writer = clientFactory.createEventWriter(inputStreamName, serializer, eventWriterConfig);

        // Prepare validation reader that will read from the stream written by the processor.
        final String validationReaderGroupName = "rg" + UUID.randomUUID().toString().replace("-", "");
        final String validationReaderId = "reader-" + UUID.randomUUID().toString();
        final ReaderConfig validationReaderConfig = ReaderConfig.builder().build();
        final ReaderGroupConfig validationReaderGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, outputStreamName))
                .build();
        @Cleanup
        final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(validationReaderGroupName, validationReaderGroupConfig);
        @Cleanup
        final EventStreamReader<TestEvent> validationReader = clientFactory.createReader(
                validationReaderId,
                validationReaderGroupName,
                new JSONSerializer<>(new TypeToken<TestEvent>(){}.getType()),
                validationReaderConfig);
        final long readTimeoutMills = 60000;
        EventStreamReaderIterator<TestEvent> readerIterator = new EventStreamReaderIterator<>(validationReader, readTimeoutMills);
        final TestEventGenerator generator = new TestEventGenerator(config.numKeys);
        final TestEventValidator validator = new TestEventValidator();
        final TestContext ctx = new TestContext(writer, readerIterator, generator, validator, workerProcessGroup, checkpointPeriodMs);
        config.func.accept(ctx);

        log.info("Cleanup");
        workerProcessGroup.close();
        validationReader.close();
        readerGroupManager.deleteReaderGroup(inputStreamReaderGroupName);
        readerGroupManager.deleteReaderGroup(validationReaderGroupName);
        streamManager.sealStream(scope, inputStreamName);
        streamManager.sealStream(scope, outputStreamName);
        streamManager.sealStream(scope, membershipSynchronizerStreamName);
        streamManager.deleteStream(scope, inputStreamName);
        streamManager.deleteStream(scope, outputStreamName);
        streamManager.deleteStream(scope, membershipSynchronizerStreamName);
    }

    /**
     * Write 20 events with 1 routing key, run 1 processor instance, and validate results.
     * @throws Exception
     */
    @Test
    public void trivialTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(1)
                .numKeys(1)
                .numInitialInstances(1)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 20, new int[]{0});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Gracefully stop 1 of 1 processor instances.
     */
    @Test
    public void gracefulRestart1of1Test() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    ctx.workerProcessGroup.stop(0);
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Gracefully stop 1 of 1 processor instances.
     * Force each events to be durably written immediately.
     */
    @Test
    public void gracefulRestart1of1DurableTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.AlwaysDurable)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    ctx.workerProcessGroup.stop(0);
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Gracefully stop 1 of 1 processor instances.
     * Do not writes events to Pravega until flushed.
     */
    @Test
    public void gracefulRestart1of1DHoldUntilFlushedTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.AlwaysHoldUntilFlushed)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    ctx.workerProcessGroup.stop(0);
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Gracefully stop 1 of 2 processor instances.
     */
    @Test
    public void gracefulStop1of2Test() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(2)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0, 1});
                    ctx.workerProcessGroup.stop(0);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Kill and restart 1 of 1 processor instances.
     * This is simulated by pausing the first instance until the test completes.
     * This may produce duplicates.
     */
    @Test
    public void killAndRestart1of1Test() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    ctx.workerProcessGroup.get(0).pause();
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    log.info("getDuplicateEventCount={}", ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Kill and restart 1 of 1 processor instances.
     * This is simulated by pausing the first instance until the test completes.
     * This tests waits for 2x the checkpoint interval to ensure that the
     * first processor writes its position before pausing.
     * This will not produce duplicates.
     */
    @Test
    public void killAndRestart1of1WhenIdleTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                    // Wait for a while to ensure that a checkpoint occurs and all events have been flushed.
                    // This will update the reader group state to indicate that this reader has read up to this point.
                    sleep(2*ctx.checkpointPeriodMs);
                    // Kill the worker instance.
                    ctx.workerProcessGroup.get(0).pause();
                    // Start a new worker instance. It should identify the dead worker and call readerOffline(null).
                    // The new worker should resume exactly where the killed worker left off, producing no duplicates.
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 19, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    @Test(timeout = 2*60*1000)
    public void handleExceptionDuringProcessTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(1)
                .numKeys(1)
                .numInitialInstances(1)
                .writeMode(WriteMode.AlwaysHoldUntilFlushed)
                .func(ctx -> {
                    // Force process function to throw an exception.
                    ctx.workerProcessGroup.get(0).induceFailureDuringProcess();
                    // Write an event.
                    writeEvents(ctx, 1);
                    // Wait for process function to throw an exception.
                    sleep(2*ctx.checkpointPeriodMs);
                    // Start a new worker instance so that we can determine where it reads from.
                    // The event should have been processed exactly once.
                    ctx.workerProcessGroup.start(1);
                    validateEvents(ctx, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    @Test(timeout = 2*60*1000)
    public void handleExceptionDuringFlushTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(1)
                .writeMode(WriteMode.AlwaysHoldUntilFlushed)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                    // Wait for a while to ensure that a checkpoint occurs and all events have been flushed.
                    // This will update the reader group state to indicate that this reader has read up to this point.
                    sleep(2*ctx.checkpointPeriodMs);
                    // Force an exception during flush.
                    ctx.workerProcessGroup.get(0).induceFailureDuringFlush();
                    // Write some events that will be processed and queued for writing (they will not be written though).
                    final int newEventCount = 3;
                    writeEvents(ctx, newEventCount);
                    // Wait for a while so that flush is called and throws an exception.
                    sleep(2*ctx.checkpointPeriodMs);
                    // Start a new worker instance so that we can determine where it reads from.
                    ctx.workerProcessGroup.start(1);
                    validateEvents(ctx, new int[]{1});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * This test processes events and then kills the processor before it receives a checkpoint.
     * This is expected to produce duplicates.
     */
    @Test(timeout = 2*60*1000)
    public void killAndRestart1of1ForcingDuplicatesTest() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(1)
                .numKeys(1)
                .numInitialInstances(1)
                .writeMode(WriteMode.AlwaysDurable)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                    // Wait for a while to ensure that a checkpoint occurs and all events have been flushed.
                    // This will update the reader group state to indicate that this reader has read up to this point.
                    sleep(2*ctx.checkpointPeriodMs);
                    // Write some events that will be processed and durably written.
                    final int expectedDuplicateEventCount = 3;
                    writeEventsAndValidate(ctx, expectedDuplicateEventCount, new int[]{0});
                    Assert.assertEquals(0, ctx.validator.getDuplicateEventCount());
                    // Kill the worker instance before it checkpoints and updates the reader group state.
                    // There is no control mechanism to prevent the checkpoint.
                    // If a checkpoint occurs here, this test will fail because duplicates will not be produced.
                    ctx.workerProcessGroup.get(0).pause();
                    // Start a new worker instance. It should identify the dead worker and call readerOffline(null).
                    // The new worker should produce duplicates.
                    ctx.workerProcessGroup.start(1);
                    writeEventsAndValidate(ctx, 90, new int[]{1});
                    Assert.assertEquals(expectedDuplicateEventCount, ctx.validator.getDuplicateEventCount());
                }).build());
    }

    /**
     * Kill 5 of 6 processor instances.
     */
    @Test
    public void kill5of6Test() throws Exception {
        run(EndToEndTestConfig.builder()
                .numSegments(6)
                .numKeys(24)
                .numInitialInstances(6)
                .writeMode(WriteMode.Default)
                .func(ctx -> {
                    writeEventsAndValidate(ctx, 100, new int[]{0, 1, 2, 3, 4, 5});
                    IntStream.rangeClosed(0, 4).forEach(i -> ctx.workerProcessGroup.get(i).pause());
                    writeEventsAndValidate(ctx, 90, new int[]{5});
                }).build());
    }
}
