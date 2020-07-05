package io.pravega.example.streamprocessing;

import java.util.Iterator;

public class TestEventValidator {
    private final TestEventGenerator generator;

    public TestEventValidator(TestEventGenerator generator) {
        this.generator = generator;
    }

    public void validate(Iterator<TestEvent> events) {

    }
}
