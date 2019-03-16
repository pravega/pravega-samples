package io.pravega.example.streamprocessing;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A simple example that demonstrates reading events from a Pravega stream, processing each event,
 * and writing each output event to another Pravega stream.
 *
 * This supports multiple worker threads.
 * Upon restart, it restarts from the last successful checkpoint and guarantees exactly-once semantics.
 *
 * Use {@link EventGenerator} to generate input events and {@link EventDebugSink}
 * to view the output events.
 *
 * See also {@link ExactlyOnceMultithreadedProcessorWorker}.
 */

public class ExactlyOnceMultithreadedProcessor implements Runnable {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ExactlyOnceMultithreadedProcessor.class);

    private static final String PRAVEGA_CHECKPOINT_FILE_NAME = "pravega-checkpoint";
    private static final String LATEST_CHECKPOINT_NAME_FILE_NAME = "latest";
    static final String CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX = "pravega-transactions-worker-";

    private final String scope;
    private final String inputStreamName;
    private final String outputStreamName;
    private final URI controllerURI;
    private final int numWorkers;
    private final String readerGroupName;
    private final ReaderGroup readerGroup;
    private final ScheduledExecutorService initiateCheckpointExecutor;
    private final ScheduledExecutorService performCheckpointExecutor;
    private final ExecutorService workerExecutor;
    private final ReaderGroupManager readerGroupManager;
    private final Path checkpointRootPath = Parameters.getCheckpointRootPath();
    private final Path latestCheckpointNamePath = checkpointRootPath.resolve(LATEST_CHECKPOINT_NAME_FILE_NAME);
    private final long checkpointPeriodMs = Parameters.getCheckpointPeriodMs();
    private final long checkpointTimeoutMs = Parameters.getCheckpointTimeoutMs();
    private final boolean startFromCheckpoint;
    private final String startFromCheckpointName;
    private final ClientFactory clientFactory;
    private final EventStreamWriter<String> writer;

    public ExactlyOnceMultithreadedProcessor(String scope, String inputStreamName, String outputStreamName, URI controllerURI, int numWorkers) throws Exception {
        this.scope = scope;
        this.inputStreamName = inputStreamName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
        this.numWorkers = numWorkers;

        ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints();

        // Load the last checkpoint.
        startFromCheckpoint = latestCheckpointNamePath.toFile().exists();
        if (startFromCheckpoint) {
            // Read the name of the checkpoint from the file /tmp/checkpoint/latest.
            String checkpointName = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(latestCheckpointNamePath))).toString();
            Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
            Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
            log.info("Reading Pravega checkpoint from {}", checkpointPath);
            Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Files.readAllBytes(checkpointPath)));
            log.info("Starting from checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());
            builder = builder.startFromCheckpoint(checkpoint);
            startFromCheckpointName = checkpointName;
        } else {
            log.warn("Checkpoint file {} not found. Starting processing from the earliest event.", checkpointRootPath);

            // Create streams.
            try (StreamManager streamManager = StreamManager.create(controllerURI)) {
                streamManager.createScope(scope);
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.byEventRate(
                                Parameters.getTargetRateEventsPerSec(),
                                Parameters.getScaleFactor(),
                                Parameters.getMinNumSegments()))
                        .build();
                streamManager.createStream(scope, inputStreamName, streamConfig);
                // Since we are starting processing from the beginning, delete and create a new output stream.
                // TODO: Should we truncate stream instead of deleting?
                try {
                    streamManager.sealStream(scope, outputStreamName);
                } catch (Exception e) {
                    if (!(e.getCause() instanceof InvalidStreamException)) {
                        throw e;
                    }
                }
                // TODO: It would be nice if deleteStream did not require sealStream to be called.
                streamManager.deleteStream(scope, outputStreamName);
                streamManager.createStream(scope, outputStreamName, streamConfig);
            }

            // Create a reader group that starts from the earliest event.
            builder = builder.stream(Stream.of(scope, inputStreamName));
            startFromCheckpointName = null;
        }

        clientFactory = ClientFactory.withScope(scope, controllerURI);
        writer = clientFactory.createEventWriter(
                outputStreamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());

        // Create a reader group manager. It must remain open to allow manual checkpoints to work.
        readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);

        final ReaderGroupConfig readerGroupConfig = builder.build();
        readerGroupName = UUID.randomUUID().toString().replace("-", "");
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        readerGroup = readerGroupManager.getReaderGroup(readerGroupName);

        initiateCheckpointExecutor = Executors.newScheduledThreadPool(1);
        performCheckpointExecutor = Executors.newScheduledThreadPool(1);
        workerExecutor = Executors.newFixedThreadPool(numWorkers);
    }

    /**
     * Commit all transactions that are part of a checkpoint.
     *
     * @param checkpointName
     */
    private void commitTransactions(String checkpointName) {
        log.info("commitTransactions: BEGIN");

        // Read the contents of all pravega-transactions-worker-XX files.
        // These files contain the Pravega transaction IDs that must be committed now.
        Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
        List<UUID> txnIds = IntStream
                .range(0, numWorkers)
                .boxed()
                .map(workerIndex -> checkpointDirPath.resolve(CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX + workerIndex))
                .flatMap(path -> {
                    try {
                        return Files.readAllLines(path, StandardCharsets.UTF_8).stream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(UUID::fromString)
                .collect(Collectors.toList());

        log.info("commitTransactions: txnIds={}", txnIds);

        // Initiate commit of all transactions in the checkpoint.
        txnIds.parallelStream().forEach(txnId -> {
            try {
                Transaction<String> transaction = writer.getTxn(txnId);
                Transaction.Status status = transaction.checkStatus();
                log.info("commitTransaction: transaction {} status is {}", transaction.getTxnId(), status);
                if (status == Transaction.Status.OPEN) {
                    log.info("commitTransaction: committing {}", transaction.getTxnId());
                    transaction.commit();
                    // Note that commit may return before the transaction is committed.
                    // TODO: It would be nice for commit() to return a future when it becomes COMMITTED or ABORTED.
                }
            } catch (TxnFailedException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for commit of all transactions in the checkpoint.
        txnIds.parallelStream().forEach(txnId -> {
            try {
                Transaction<String> transaction = writer.getTxn(txnId);
                // TODO: Is there a better way to wait for COMMITTED besides polling?
                for (; ; ) {
                    Transaction.Status status = transaction.checkStatus();
                    log.info("commitTransaction: transaction {} status is {}", transaction.getTxnId(), status);
                    if (status == Transaction.Status.COMMITTED) {
                        log.info("commitTransaction: committed {}", transaction.getTxnId());
                        break;
                    } else if (status == Transaction.Status.ABORTED) {
                        throw new RuntimeException(new TxnFailedException());
                    }
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        log.info("commitTransactions: END");
    }

    public void run() {
        // It is possible that the checkpoint was completely written but that some or all Pravega transactions
        // have not been committed. This will ensure that they are.
        if (startFromCheckpoint) {
            commitTransactions(startFromCheckpointName);
        }

        // Schedule periodic task to initiate checkpoints.
        // If any execution of this task takes longer than its period, then subsequent executions may start late, but will not concurrently execute.
        initiateCheckpointExecutor.scheduleAtFixedRate(this::performCheckpoint, checkpointPeriodMs, checkpointPeriodMs, TimeUnit.MILLISECONDS);

        // Start workers.
        IntStream.range(0, numWorkers).forEach(workerIndex -> {
            ExactlyOnceMultithreadedProcessorWorker worker = new ExactlyOnceMultithreadedProcessorWorker(workerIndex, scope, readerGroupName, startFromCheckpointName, outputStreamName, controllerURI);
            workerExecutor.submit(worker);
        });
        try {
            workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Initiate a checkpoint, wait for it to complete, and write the checkpoint to the state.
     */
    private void performCheckpoint() {
        final String checkpointName = UUID.randomUUID().toString();
        log.info("performCheckpoint: BEGIN: checkpointName={}", checkpointName);
        try {
            Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
            checkpointDirPath.toFile().mkdirs();

            final Set<String> onlineReaders = readerGroup.getOnlineReaders();
            log.info("performCheckpoint: onlineReaders ({})={}", onlineReaders.size(), onlineReaders);
            log.info("performCheckpoint: Calling initiateCheckpoint; checkpointName={}", checkpointName);
            CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, performCheckpointExecutor);
            Checkpoint checkpoint = checkpointFuture.get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
            // At this point, all workers have received and processed the checkpoint.
            log.info("performCheckpoint: Checkpoint completed; checkpointName={}, positions={}", checkpointName, checkpoint.asImpl().getPositions());

            Path checkpointPath = checkpointDirPath.resolve(PRAVEGA_CHECKPOINT_FILE_NAME);
            log.info("Writing Pravega checkpoint to {}", checkpointPath);
            try (FileOutputStream fos = new FileOutputStream(checkpointPath.toFile())) {
                fos.write(checkpoint.toBytes().array());
                fos.flush();
                fos.getFD().sync();
            }

            // Create "latest" file that indicates the latest checkpoint name.
            // This file must be updated atomically.
            Path latestTmpCheckpointPath = checkpointRootPath.resolve(LATEST_CHECKPOINT_NAME_FILE_NAME + ".tmp");
            try (FileOutputStream fos = new FileOutputStream(latestTmpCheckpointPath.toFile())) {
                fos.write(checkpointName.getBytes(StandardCharsets.UTF_8));
                fos.flush();
                fos.getFD().sync();
            }
            Files.move(latestTmpCheckpointPath, latestCheckpointNamePath, StandardCopyOption.ATOMIC_MOVE);

            // Read list of TxnIds from checkpoint directory written by all workers and commit all transactions.
            commitTransactions(checkpointName);

            cleanCheckpointDirectory(checkpointDirPath);
        } catch (final Exception e) {
            log.warn("performCheckpoint: timed out waiting for checkpoint to complete", e);
            // Ignore error. We will retry when we are scheduled again.
        }
        log.info("performCheckpoint: END: checkpointName={}", checkpointName);
    }

    /**
     * Delete everything in the checkpoint root path (/tmp/checkpoint) except the "latest" file and
     * the latest checkpoint directory.
     *
     * @param keepCheckpointDirPath The latest checkpoint directory which will not be deleted.
     */
    private void cleanCheckpointDirectory(Path keepCheckpointDirPath) {
        try {
            Files.walkFileTree(checkpointRootPath,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes basicFileAttributes) throws IOException {
                            if (dir.equals(keepCheckpointDirPath)) {
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            if (!dir.equals(checkpointRootPath)) {
                                Files.delete(dir);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (!file.equals(latestCheckpointNamePath)) {
                                Files.delete(file);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            log.warn("cleanCheckpointDirectory", e);
        }
    }

    public static void main(String[] args) throws Exception {
        ExactlyOnceMultithreadedProcessor master = new ExactlyOnceMultithreadedProcessor(
                Parameters.getScope(),
                Parameters.getStream1Name(),
                Parameters.getStream2Name(),
                Parameters.getControllerURI(),
                Parameters.getNumWorkers());
        master.run();
    }
}
