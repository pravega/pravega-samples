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

import com.google.common.collect.ImmutableMap;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for TestEventValidator.
 */
public class TestEventValidatorUnitTests {
    static final Logger log = LoggerFactory.getLogger(TestEventValidatorUnitTests.class);

    @Test
    public void exactlyOnceTest() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),
                new TestEvent(0, 1L, 10),
                new TestEvent(0, 2L, 11),
                new TestEvent(0, 3L, 11),
                new TestEvent(1, 2L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers = ImmutableMap.of(
                0, 3L,
                1, 2L);
        Map<Integer, Long> expectedEventCountByInstanceId = ImmutableMap.of(
                10, 4L,
                11, 3L);
        TestEventValidator validator = new TestEventValidator();
        validator.validate(events.iterator(), expectedLastSequenceNumbers);
        Assert.assertEquals(expectedEventCountByInstanceId, validator.getEventCountByInstanceId());
        Assert.assertEquals(0, validator.getDuplicateEventCount());
    }

    @Test
    public void duplicateEventTest() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),
                new TestEvent(0, 0L, 11),   // duplicate
                new TestEvent(0, 1L, 11),
                new TestEvent(0, 2L, 11),
                new TestEvent(0, 3L, 11),
                new TestEvent(1, 2L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers = ImmutableMap.of(
                0, 3L,
                1, 2L);
        Map<Integer, Long> expectedEventCountByInstanceId = ImmutableMap.of(
                10, 3L,
                11, 4L);
        TestEventValidator validator = new TestEventValidator();
        validator.validate(events.iterator(), expectedLastSequenceNumbers);
        Assert.assertEquals(expectedEventCountByInstanceId, validator.getEventCountByInstanceId());
        Assert.assertEquals(1, validator.getDuplicateEventCount());
    }

    @Test
    public void rewindTest() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),
                new TestEvent(1, 0L, 11), // rewind, duplicate
                new TestEvent(1, 1L, 11), // duplicate
                new TestEvent(0, 1L, 11),
                new TestEvent(0, 2L, 11),
                new TestEvent(0, 3L, 11),
                new TestEvent(1, 2L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers = ImmutableMap.of(
                0, 3L,
                1, 2L);
        Map<Integer, Long> expectedEventCountByInstanceId = ImmutableMap.of(
                10, 3L,
                11, 4L);
        TestEventValidator validator = new TestEventValidator();
        validator.validate(events.iterator(), expectedLastSequenceNumbers);
        Assert.assertEquals(expectedEventCountByInstanceId, validator.getEventCountByInstanceId());
        Assert.assertEquals(2, validator.getDuplicateEventCount());
    }

    @Test
    public void clearCountersTest() {
        List<TestEvent> events1 = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),
                new TestEvent(1, 0L, 11), // rewind, duplicate
                new TestEvent(1, 1L, 11), // duplicate
                new TestEvent(0, 1L, 11),
                new TestEvent(0, 2L, 11),
                new TestEvent(0, 3L, 11),
                new TestEvent(1, 2L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers1 = ImmutableMap.of(
                0, 3L,
                1, 2L);
        TestEventValidator validator = new TestEventValidator();
        validator.validate(events1.iterator(), expectedLastSequenceNumbers1);
        validator.clearCounters();
        List<TestEvent> events2 = Arrays.asList(
                new TestEvent(100, 0L, 10),
                new TestEvent(100, 0L, 10), // duplicate
                new TestEvent(100, 1L, 10));
        Map<Integer, Long> expectedLastSequenceNumbers2 = ImmutableMap.of(
                100, 1L);
        Map<Integer, Long> expectedEventCountByInstanceId2 = ImmutableMap.of(
                10, 2L);
        validator.validate(events2.iterator(), expectedLastSequenceNumbers2);
        Assert.assertEquals(expectedEventCountByInstanceId2, validator.getEventCountByInstanceId());
        Assert.assertEquals(1, validator.getDuplicateEventCount());
    }

    @Test
    public void missingEventTest() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),
                new TestEvent(0, 2L, 11),   // missing prior event
                new TestEvent(0, 3L, 11),
                new TestEvent(1, 2L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers = ImmutableMap.of(
                0, 3L,
                1, 2L);
        TestEventValidator validator = new TestEventValidator();
        AssertExtensions.assertThrows(MissingEventException.class, () -> validator.validate(events.iterator(), expectedLastSequenceNumbers));
    }

    @Test
    public void missingFinalEventTest() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(0, 0L, 10),
                new TestEvent(1, 0L, 10),
                new TestEvent(1, 1L, 10),   // missing following event
                new TestEvent(0, 1L, 10),
                new TestEvent(0, 2L, 11),
                new TestEvent(0, 3L, 11));
        Map<Integer, Long> expectedLastSequenceNumbers = ImmutableMap.of(
                0, 3L,
                1, 2L);
        TestEventValidator validator = new TestEventValidator();
        AssertExtensions.assertThrows(NoMoreEventsException.class, () -> validator.validate(events.iterator(), expectedLastSequenceNumbers));
    }
}
