package io.pravega.utils;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;

import java.util.Iterator;
import java.util.Optional;

public class EventStreamReaderIterator<T> implements Iterator<T> {
    private final EventStreamReader<T> reader;
    private final long timeoutMillis;
    private Optional<T> nextEvent = Optional.empty();

    public EventStreamReaderIterator(EventStreamReader<T> reader, long timeoutMillis) {
        this.reader = reader;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean hasNext() {
        readIfNeeded();
        return nextEvent.isPresent();
    }

    @Override
    public T next() {
        readIfNeeded();
        if (nextEvent.isPresent()) {
            final T event = nextEvent.get();
            nextEvent = Optional.empty();
            return event;
        } else {
            throw new RuntimeException("Timeout");
        }
    }

    private void readIfNeeded() {
        if (!nextEvent.isPresent()) {
            for (; ; ) {
                final EventRead<T> eventRead = reader.readNextEvent(timeoutMillis);
                if (!eventRead.isCheckpoint()) {
                    if (eventRead.getEvent() != null) {
                        nextEvent = Optional.of(eventRead.getEvent());
                    }
                    return;
                }
            }
        }
    }
}
