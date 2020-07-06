package io.pravega.example.streamprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestEventValidator {
    static final Logger log = LoggerFactory.getLogger(TestEventValidator.class);

    private final TestEventGenerator generator;
    // map from routing key to sequence number
    private final Map<Integer, Long> receivedSequenceNumbers = new HashMap<>();

    // map from instanceId to count of events
    private final Map<Integer, Long> eventCountByInstanceId = new HashMap<>();

    public TestEventValidator(TestEventGenerator generator) {
        this.generator = generator;
    }

    public void validate(Iterator<TestEvent> events) {
        // pendingSequenceNumbers contains a map from key to sequence number for events that have been generated but not yet received by validate.
        // A key is removed when all events up to the generated sequence number for that key are received.
        final Map<Integer, Long> pendingSequenceNumbers = new HashMap<>();
        final Map<Integer, Long> generatedSequenceNumbers = generator.getLastSequenceNumbers();
        log.info("generatedSequenceNumbers={}", generatedSequenceNumbers);
        generatedSequenceNumbers.forEach((key, sequenceNumber) -> {
            if (receivedSequenceNumbers.getOrDefault(key, -1L) < sequenceNumber) {
                pendingSequenceNumbers.put(key, sequenceNumber);
            }
        });
        log.info("pendingSequenceNumbers={}", pendingSequenceNumbers);
        while (events.hasNext()) {
            final TestEvent event = events.next();
            final long lastReceivedSequenceNumber = receivedSequenceNumbers.getOrDefault(event.key, -1L);
            log.info("event={}, lastReceivedSequenceNumber={}", event, lastReceivedSequenceNumber);
            if (event.sequenceNumber <= lastReceivedSequenceNumber) {
                throw new IllegalStateException("Duplicate event");
            } else if (event.sequenceNumber > lastReceivedSequenceNumber + 1) {
                throw new IllegalStateException("Gap");
            } else {
                receivedSequenceNumbers.put(event.key, event.sequenceNumber);
                eventCountByInstanceId.merge(event.processedByInstanceId, 1L, Long::sum);   // increment counter
                if (pendingSequenceNumbers.getOrDefault(event.key, -1L) <= event.sequenceNumber) {
                    pendingSequenceNumbers.remove(event.key);
                    if (pendingSequenceNumbers.size() == 0) {
                        // All data received.
                        log.info("All data received; receivedSequenceNumbers={}", receivedSequenceNumbers);
                        return;
                    }
                }
            }
        }
        throw new IllegalStateException("No more events");
    }

    public Map<Integer, Long> getEventCountByInstanceId() {
        return eventCountByInstanceId;
    }
}
