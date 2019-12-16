/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 */
package io.pravega.turbineheatsensor;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.commons.cli.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class TurbineHeatSensor {

    private static final String DEFAULT_SCOPE_NAME = "examples";
    private static final String DEFAULT_STREAM_NAME = "turbineHeatTest";

    private static PerfStats produceStats, consumeStats;
    private static String controllerUri = "tcp://127.0.0.1:9090";
    private static int messageSize = 100;
    private static String streamName = DEFAULT_STREAM_NAME;
    private static String scopeName = DEFAULT_SCOPE_NAME;

    private static StreamManager streamManager;
    private static ReaderGroupManager readerGroupManager;

    private static boolean onlyWrite = false;
    private static boolean blocking = false;
    // How many producers should we run concurrently
    private static int producerCount = 20;
    // How many events each producer has to produce per seconds
    private static int eventsPerSec = 40;
    // How long it needs to run
    private static int runtimeSec = 10;
    // Should producers use Transaction or not
    private static boolean isTransaction = false;
    private static int reportingInterval = 200;

    private static final long DEFAULT_TXN_TIMEOUT_MS = 30000L;


    public static void main(String[] args) throws Exception {

        // Place names where wind farms are located
        String[] locations = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
                "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
                "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
                "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
                "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
                "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
                "West Virginia", "Wisconsin", "Wyoming", "Montgomery", "Juneau", "Phoenix", "Little Rock",
                "Sacramento", "Denver", "Hartford", "Dover", "Tallahassee", "Atlanta", "Honolulu", "Boise",
                "Springfield", "Indianapolis", "Des Moines", "Topeka", "Frankfort", "Baton Rouge", "Augusta",
                "Annapolis", "Boston", "Lansing", "St. Paul", "Jackson", "Jefferson City", "Helena", "Lincoln",
                "Carson City", "Concord", "Trenton", "Santa Fe", "Albany", "Raleigh", "Bismarck", "Columbus",
                "Oklahoma City", "Salem", "Harrisburg", "Providence", "Columbia", "Pierre", "Nashville", "Austin",
                "Salt Lake City", "Montpelier", "Richmond", "Olympia", "Charleston", "Madison", "Cheyenne"};

        parseCmdLine(args);

        System.out.println("\nTurbineHeatSensor is running "+ producerCount + " simulators each ingesting " +
                eventsPerSec + " temperature data per second for " + runtimeSec + " seconds " +
                (isTransaction ? "via transactional mode" : " via non-transactional mode. The controller end point " +
                        "is " + controllerUri));

        // Initialize executor
        ExecutorService executor = Executors.newFixedThreadPool(producerCount + 10);

        URI controllerUri;
        EventStreamClientFactory clientFactory;
        try {
            controllerUri = new URI(TurbineHeatSensor.controllerUri);
            clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerUri).build());
            streamManager = StreamManager.create(controllerUri);
            readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerUri);

            streamManager.createScope(scopeName);

            ScalingPolicy policy = ScalingPolicy.fixed(producerCount);
            StreamConfiguration config = StreamConfiguration.builder()
                    .scalingPolicy(policy)
                    .build();
            streamManager.createStream(scopeName, streamName, config);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        produceStats = new PerfStats(producerCount * eventsPerSec * runtimeSec, reportingInterval, messageSize);

        if ( !onlyWrite ) {
            consumeStats = new PerfStats(producerCount * eventsPerSec * runtimeSec, reportingInterval, messageSize);
            SensorReader reader = new SensorReader(producerCount * eventsPerSec * runtimeSec, clientFactory);
            executor.execute(reader);
        }
        /* Create producerCount number of threads to simulate sensors. */
        Instant startEventTime = Instant.EPOCH.plus(8, ChronoUnit.HOURS); // sunrise
        for (int i = 0; i < producerCount; i++) {
            double baseTemperature = locations[i % locations.length].length() * 10;
            TemperatureSensor sensor =
                    new TemperatureSensor(i, locations[i % locations.length], baseTemperature, 20, startEventTime);
            TemperatureSensors worker;
            if (isTransaction) {
                worker = new TransactionTemperatureSensors(sensor, eventsPerSec, runtimeSec, isTransaction, clientFactory);
            } else {
                worker = new TemperatureSensors(sensor, eventsPerSec, runtimeSec, isTransaction, clientFactory);
            }
            executor.execute(worker);
        }

        executor.shutdown();
        // Wait until all threads are finished.
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("\nFinished all producers");
        produceStats.printAll();
        produceStats.printTotal();
        if ( !onlyWrite ) {
            consumeStats.printTotal();
        }
        clientFactory.close();
//        ZipKinTracer.getTracer().close();
        System.exit(0);
    }

    private static void parseCmdLine(String[] args) {
        // create Options object
        Options options = new Options();

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("eventspersec", true, "number events per sec");
        options.addOption("runtime", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message");
        options.addOption("stream", true, "Stream name");
        options.addOption("writeonly", true, "Just produce vs read after produce");
        options.addOption("blocking", true, "Block for each ack");
        options.addOption("reporting", true, "Reporting internval");

        options.addOption("help", false, "Help message");

        CommandLineParser parser = new BasicParser();
        try {

            CommandLine commandline = parser.parse(options, args);
            // Since it is command line sample producer, user inputs will be accepted from console
            if (commandline.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("TurbineHeatSensor", options);
                System.exit(0);
            } else {

                if (commandline.hasOption("controller")) {
                    controllerUri = commandline.getOptionValue("controller");
                }

                if (commandline.hasOption("producers")) {
                    producerCount = Integer.parseInt(commandline.getOptionValue("producers"));
                }

                if (commandline.hasOption("eventspersec")) {
                    eventsPerSec = Integer.parseInt(commandline.getOptionValue("eventspersec"));
                }

                if (commandline.hasOption("runtime")) {
                    runtimeSec = Integer.parseInt(commandline.getOptionValue("runtime"));
                }

                if (commandline.hasOption("transaction")) {
                    isTransaction = Boolean.parseBoolean(commandline.getOptionValue("transaction"));
                }

                if (commandline.hasOption("size")) {
                    messageSize = Integer.parseInt(commandline.getOptionValue("size"));
                }

                if (commandline.hasOption("stream")) {
                    streamName = commandline.getOptionValue("stream");
                }

                if (commandline.hasOption("writeonly")) {
                    onlyWrite = Boolean.parseBoolean(commandline.getOptionValue("writeonly"));
                }

                if (commandline.hasOption("blocking")) {
                    blocking = Boolean.parseBoolean(commandline.getOptionValue("blocking"));
                }

                if (commandline.hasOption("reporting")) {
                    reportingInterval = Integer.parseInt(commandline.getOptionValue("reporting"));
                }

                if (commandline.hasOption("transaction")) {
                    isTransaction = Boolean.parseBoolean(commandline.getOptionValue("transaction"));
                }
            }
        } catch (Exception nfe) {
            nfe.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * A stream of sensor events.
     */
    private static class TemperatureSensor implements Iterator<SensorEvent> {

        private final int sensorId;
        private final String city;
        private final double bias;
        private final double magnitude;
        private final Instant startTime;
        private long offset;

        public TemperatureSensor(int sensorId, String city, double bias, double magnitude, Instant startTime) {
            this.sensorId = sensorId;
            this.city = city;
            this.bias = bias;
            this.magnitude = magnitude;
            this.startTime = startTime;
        }

        public int getSensorId() {
            return sensorId;
        }

        public String getCity() {
            return city;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public SensorEvent next() {
            // simulate a normal daily temperature curve
            double temperature = magnitude * Math.sin((1.0/24) * 2 * Math.PI * offset) + bias;
            return new SensorEvent(startTime.plus(offset++, ChronoUnit.HOURS), temperature);
        }
    }

    public static class SensorEvent {
        private Instant timestamp;
        private double temperature;

        public SensorEvent(Instant timestamp, double temperature) {
            this.timestamp = timestamp;
            this.temperature = temperature;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public double getTemperature() {
            return temperature;
        }
    }

    /**
     * A Sensor simulator class that generates dummy value as temperature measurement and ingests to specified stream.
     */

    private static class TemperatureSensors implements Runnable {

        final JavaSerializer<String> SERIALIZER = new JavaSerializer<>();

        final EventStreamWriter<String> producer;
        private final TemperatureSensor sensor;
        private final int eventsPerSec;
        private final int secondsToRun;
        private final boolean isTransaction;

        TemperatureSensors(TemperatureSensor sensor, int eventsPerSec, int secondsToRun, boolean isTransaction,
                           EventStreamClientFactory factory) {
            this.sensor = sensor;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.isTransaction = isTransaction;
            this.producer = factory.createEventWriter(streamName, SERIALIZER, EventWriterConfig.builder().build());
        }

        /**
         * This function will be executed in a loop and time behavior is measured.
         * @return A function which takes String key and data and returns a future object.
         */
        BiFunction<String, String, Future> sendFunction() {
            return  (key, data) -> producer.writeEvent(key, data);
        }

        public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return future.get();
                } catch (InterruptedException|ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        /**
         * Executes the given method over the producer with configured settings.
         * @param fn The function to execute.
         */
        void runLoop(BiFunction<String, String, Future> fn) {

            int producerId = sensor.getSensorId();
            String city = sensor.getCity();

            Future<Void> retFuture = null;
            for (int i = 0; i < secondsToRun; i++) {
                int currentEventsPerSec = 0;

                long loopStartTime = System.currentTimeMillis();
                while ( currentEventsPerSec < eventsPerSec) {
                    currentEventsPerSec++;

                    // Construct event payload
                    SensorEvent event = sensor.next();
                    String val = event.getTimestamp().toEpochMilli() + ", " + producerId + ", " + city + ", " + (int) event.getTemperature();
                    String payload = String.format("%-" + messageSize + "s", val);
                    // event ingestion
                    long now = System.currentTimeMillis();
                    retFuture = produceStats.runAndRecordTime(() -> {
                                return this.makeCompletableFuture(fn.apply(Integer.toString(producerId),
                                        payload));
                            },
                            now,
                            payload.length());
                    //If it is a blocking call, wait for the ack
                    if ( blocking ) {
                        try {
                            retFuture.get();
                        } catch (InterruptedException  | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }

                }
                long timeSpent = System.currentTimeMillis() - loopStartTime;
                // wait for next event
                try {
                    //There is no need for sleep for blocking calls.
                    if ( !blocking ) {
                        if ( timeSpent < 1000) {
                            Thread.sleep((1000 - timeSpent) / 1000 );
                        }
                    }
                } catch (InterruptedException e) {
                    // log exception
                    System.exit(1);
                }
            }
            producer.close();
            try {
                //Wait for the last packet to get acked
                retFuture.get();
            } catch (InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            runLoop(sendFunction());
        }
    }


    private static class TransactionTemperatureSensors extends TemperatureSensors {

        final TransactionalEventStreamWriter<String> producerTxn;
        private final Transaction<String> transaction;

        TransactionTemperatureSensors(TemperatureSensor sensor, int eventsPerSec, int secondsToRun, boolean
                isTransaction, EventStreamClientFactory factory) {
            super(sensor, eventsPerSec, secondsToRun, isTransaction, factory);
            EventWriterConfig eventWriterConfig =  EventWriterConfig.builder()
                    .transactionTimeoutTime(DEFAULT_TXN_TIMEOUT_MS)
                    .build();
            this.producerTxn = factory.createTransactionalEventWriter(streamName, SERIALIZER, eventWriterConfig);
            transaction = producerTxn.beginTxn();
        }

        BiFunction<String, String, Future> sendFunction() {
            return  ( key, data) -> {
                try {
                    transaction.writeEvent(key, data);
                } catch (TxnFailedException e) {
                    System.out.println("Publish to transaction failed");
                    e.printStackTrace();
                }
                return CompletableFuture.completedFuture(null);
            };
        }
    }

    /**
     * A Sensor reader class that reads the temperative data
     */
    private static class SensorReader implements Runnable {

        private final JavaSerializer<String> SERIALIZER = new JavaSerializer<>();
        final EventStreamReader<String> reader;
        private int totalEvents;

        public SensorReader(int totalEvents, EventStreamClientFactory clientFactory) {
        	this.totalEvents = totalEvents;
        	reader = createReader(clientFactory);
        }

        @Override
        public void run() {
            try {
                do {
                    try {
                        final EventRead<String> result = reader.readNextEvent(0);
                        if (result.getEvent() != null) {
                            consumeStats.runAndRecordTime(() -> {
                                return CompletableFuture.completedFuture(null);
                            }, Long.parseLong(result.getEvent().split(",")[0]), 100);
                            totalEvents--;
                        }
                    } catch (ReinitializationRequiredException e) {
                        e.printStackTrace();
                    }
                } while (totalEvents > 0);
            }
            finally {
                reader.close();
            }
        }

        public EventStreamReader<String> createReader(EventStreamClientFactory clientFactory) {
            String readerName = "Reader";

            //reusing a reader group name doesn't work (probably because the sequence is already consumed)
            //until we figure out how to manage this, use a random reader group name
            String readerGroup = UUID.randomUUID().toString().replace("-", "");
            ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scopeName, streamName))
                    .build();
            readerGroupManager.createReaderGroup(readerGroup, groupConfig);
            ReaderConfig readerConfig = ReaderConfig.builder().build();
            return clientFactory.createReader(readerName, readerGroup, SERIALIZER, readerConfig);
        }
    }
}
