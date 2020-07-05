package io.pravega.utils;

import io.pravega.client.stream.EventStreamReader;

import java.util.Iterator;

public class EventStreamReaderIterator<T> implements Iterator<T> {
    private final EventStreamReader<T> reader;
    private final long timeoutMillis;

    public EventStreamReaderIterator(EventStreamReader<T> reader, long timeoutMillis) {
        this.reader = reader;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public T next() {
        return reader.readNextEvent(timeoutMillis).getEvent();
    }
}
