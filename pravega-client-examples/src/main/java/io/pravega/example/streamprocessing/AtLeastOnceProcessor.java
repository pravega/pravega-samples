package io.pravega.example.streamprocessing;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

abstract public class AtLeastOnceProcessor implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(NonRecoverableSingleThreadedProcessor.class);

    private final long readTimeoutMillis;

    public AtLeastOnceProcessor() {
        readTimeoutMillis = 1000;
    }

    @Override
    public Void call() throws Exception {
        // TODO: handle case when reader dies (if checkpoint timeout occurs, reset reader group to last successful checkpoint)
        try (final EventStreamReader<String> reader = createReader()) {
            for (; ; ) {
                final EventRead<String> eventRead = reader.readNextEvent(readTimeoutMillis);
                log.info("call: eventRead={}", eventRead);
                if (eventRead.isCheckpoint()) {
                    flush(eventRead);
                } else if (eventRead.getEvent() != null) {
                    write(eventRead);
                }
            }
        }
    }

    abstract public EventStreamReader<String> createReader();

    abstract public void write(EventRead<String> eventRead);

    public void flush(EventRead<String> eventRead) {
    }
}
