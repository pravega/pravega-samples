package io.pravega.example.streamprocessing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestEventGenerator implements Iterator<TestEvent> {
    private final int numKeys;
    // map from routing key to sequence number
    private final Map<Integer, Long> lastSequenceNumbers;

    public TestEventGenerator(int numKeys) {
        this.numKeys = numKeys;
        lastSequenceNumbers = new HashMap<Integer, Long>();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public TestEvent next() {
        return new TestEvent(0, "0");
    }
}
