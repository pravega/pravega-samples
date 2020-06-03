package io.pravega.example.streamprocessing;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

abstract public class AtLeastOnceProcessor implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnceProcessor.class);

    private final ReaderGroup readerGroup;
    private final Serializer<String> serializer;
    private final ReaderConfig readerConfig;
    private final EventStreamClientFactory eventStreamClientFactory;
    private final SynchronizerClientFactory synchronizerClientFactory;
    private final ScheduledExecutorService executor;
    private final long heartbeatIntervalMillis;
    private final long readTimeoutMillis;




    @Override
    public Void call() throws Exception {
        // TODO: handle case when reader dies (if checkpoint timeout occurs, reset reader group to last successful checkpoint)
        final String readerId = UUID.randomUUID().toString();
        try (final ReaderGroupPruner pruner = ReaderGroupPruner.create(
                readerGroup,
                readerId,
                synchronizerClientFactory,
                executor,
                heartbeatIntervalMillis)) {
            try (final EventStreamReader<String> reader = eventStreamClientFactory.createReader(
                    readerId,
                    readerGroup.getGroupName(),
                    serializer,
                    readerConfig)) {
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
    }

    abstract public void write(EventRead<String> eventRead);

    public void flush(EventRead<String> eventRead) {
    }
}
