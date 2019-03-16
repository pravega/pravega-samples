package io.pravega.example.streamprocessing;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * See {@link ExactlyOnceMultithreadedProcessor}.
 */
public class ExactlyOnceMultithreadedProcessorWorker implements Runnable {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ExactlyOnceMultithreadedProcessorWorker.class);

    private static final String STATE_FILE_NAME_PREFIX = "state-worker-";
    private static final int READER_TIMEOUT_MS = 2000;

    private final int workerIndex;
    private final String scope;
    private final String readerGroupName;
    private final boolean startFromCheckpoint;
    private final String startFromCheckpointName;
    private final String outputStreamName;
    private final URI controllerURI;
    private final String readerId;
    private final Path checkpointRootPath = Parameters.getCheckpointRootPath();

    private State state;

    public ExactlyOnceMultithreadedProcessorWorker(int workerIndex, String scope, String readerGroupName, String startFromCheckpointName, String outputStreamName, URI controllerURI) {
        this.workerIndex = workerIndex;
        this.scope = scope;
        this.readerGroupName = readerGroupName;
        this.startFromCheckpointName = startFromCheckpointName;
        this.outputStreamName = outputStreamName;
        this.controllerURI = controllerURI;
        readerId = "worker-" + this.workerIndex;
        startFromCheckpoint = startFromCheckpointName != null;
    }

    public void run() {
        Thread.currentThread().setName("worker-" + workerIndex);
        log.info("BEGIN");

        try {
            // Load state from checkpoint.
            if (startFromCheckpoint) {
                Path checkpointDirPath = checkpointRootPath.resolve(startFromCheckpointName);
                Path statePath = checkpointDirPath.resolve(STATE_FILE_NAME_PREFIX + this.workerIndex);
                log.info("statePath={}", statePath.toString());
                try (FileInputStream fis = new FileInputStream(statePath.toString());
                     ObjectInputStream ois = new ObjectInputStream(fis)) {
                    state = (State) ois.readObject();
                }
                log.info("Loaded state {} from {}", state, statePath);
            } else {
                log.info("Initializing with new state");
                state = new State();
            }

            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<String> reader = clientFactory.createReader(
                         readerId,
                         readerGroupName,
                         new UTF8StringSerializer(),
                         ReaderConfig.builder().build());
                 EventStreamWriter<String> writer = clientFactory.createEventWriter(
                         outputStreamName,
                         new UTF8StringSerializer(),
                         EventWriterConfig.builder()
                                 .transactionTimeoutTime(Parameters.getTransactionTimeoutMs())
                                 .build())) {

                Transaction<String> transaction = null;
                long eventCounter = 0;

                for (; ; ) {
                    // Read input event.
                    EventRead<String> eventRead = reader.readNextEvent(READER_TIMEOUT_MS);
                    log.debug("readEvents: eventRead={}", eventRead);

                    if (eventRead.isCheckpoint()) {
                        // Note that next call readNextEvent will indicate to Pravega that we are done with the checkpoint.
                        String checkpointName = eventRead.getCheckpointName();
                        log.info("Got checkpoint {}", eventRead.getCheckpointName());
                        Path checkpointDirPath = checkpointRootPath.resolve(checkpointName);
                        Path transactionIdFilePath = checkpointDirPath.resolve(ExactlyOnceMultithreadedProcessor.CHECKPOINT_TRANSACTION_ID_FILE_NAME_PREFIX + this.workerIndex);

                        // Must ensure that txnId is persisted to latest state before committing transaction!
                        // Do not commit transaction here. Instead write TxnId to checkpoint directory. Master will read all TxnIds and commit transactions.

                        String transactionIds = "";
                        if (transaction != null) {
                            transaction.flush();
                            transactionIds = transaction.getTxnId().toString();
                            transaction = null;
                        }
                        Files.write(transactionIdFilePath, transactionIds.getBytes(StandardCharsets.UTF_8));

                        // Write state to checkpoint directory
                        Path statePath = checkpointDirPath.resolve(STATE_FILE_NAME_PREFIX + this.workerIndex);
                        log.info("statePath={}", statePath.toString());
                        try (FileOutputStream fos = new FileOutputStream(statePath.toString());
                             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                            oos.writeObject(state);
                            oos.flush();
                            fos.getFD().sync();
                        }

                    } else if (eventRead.getEvent() != null) {
                        eventCounter++;
                        log.debug("Read eventCounter={}, event={}", String.format("%06d", eventCounter), eventRead.getEvent());

                        if (transaction == null) {
                            transaction = writer.beginTxn();
                        }

                        // Parse input event.
                        String[] cols = eventRead.getEvent().split(",");
                        long generatedEventCounter = Long.parseLong(cols[0]);
                        String routingKey = cols[1];
                        long intData = Long.parseLong(cols[2]);
                        long generatedSum = Long.parseLong(cols[3]);
                        String generatedTimestampStr = cols[4];

                        // Process the input event and update the state.
                        state.sum += intData;
                        String processedTimestampStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date());

                        // Build the output event.
                        String message = String.join(",",
                                String.format("%06d", generatedEventCounter),
                                String.format("%06d", eventCounter),
                                routingKey,
                                String.format("%02d", intData),
                                String.format("%08d", generatedSum),
                                String.format("%08d", state.sum),
                                String.format("%03d", workerIndex),
                                generatedTimestampStr,
                                processedTimestampStr,
                                transaction.getTxnId().toString());

                        // Write the output event.
                        log.info("eventCounter={}, event={}",
                                String.format("%06d", eventCounter),
                                message);
                        transaction.writeEvent(routingKey, message);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fatal Error", e);
            System.exit(1);
        }
    }
}
