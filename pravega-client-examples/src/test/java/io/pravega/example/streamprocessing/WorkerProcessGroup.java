package io.pravega.example.streamprocessing;

import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Builder
public class WorkerProcessGroup implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerProcessGroup.class);

    private final WorkerProcessConfig config;
    private final Map<Integer, WorkerProcess> workers = new HashMap<>();

    /**
     * Streams are guaranteed to exist after calling this method.
     */
    public void start(int... instanceIds) {
        IntStream.of(instanceIds).forEach(instanceId -> {
            log.info("start: instanceId={}", instanceId);
            workers.putIfAbsent(instanceId, WorkerProcess.builder().config(config).instanceId(instanceId).build());
        });
        IntStream.of(instanceIds).parallel().forEach(instanceId -> {
            final WorkerProcess worker = workers.get(instanceId);
            worker.init();
            worker.startAsync();
        });
    }

    /**
     * Processors are guaranteed to not process events written after this method returns.
     */
    public void pause(int... instanceIds) {
        IntStream.of(instanceIds).parallel().forEach(instanceId -> {
            workers.get(instanceId).pause();
        });
    }

    /**
     * Processors are guaranteed to not process events after this method returns.
     */
    public void stop(int... instanceIds) {
        IntStream.of(instanceIds).forEach(instanceId -> {
            workers.get(instanceId).stopAsync();
        });
        IntStream.of(instanceIds).forEach(instanceId -> {
            workers.get(instanceId).awaitTerminated();
            workers.remove(instanceId);
        });
    }

    public void stopAll() {
        log.info("stopAll: workers={}", workers);
        stop(workers.keySet().stream().mapToInt(i -> i).toArray());
    }

    @Override
    public void close() throws Exception {
        stopAll();
    }
}
