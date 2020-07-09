/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.utils;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.local.InProcPravegaCluster;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility functions for creating an in-process Pravega server or connecting to an external Pravega server.
 */
@NotThreadSafe
public final class SetupUtils {
    static Logger log = LoggerFactory.getLogger(SetupUtils.class);
    private static final ScheduledExecutorService DEFAULT_SCHEDULED_EXECUTOR_SERVICE = ExecutorServiceHelpers.newScheduledThreadPool(3, "SetupUtils");

    private final PravegaGateway gateway;

    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // auth enabled by default. Set it to false to disable Pravega authentication and authorization.
    @Setter
    private boolean enableAuth = false;

    // Set to true to enable TLS
    @Setter
    private boolean enableTls = false;

    @Setter
    private boolean enableHostNameValidation = false;

    private boolean enableRestServer = true;

    // The test Scope name.
    @Getter
    private final String scope = RandomStringUtils.randomAlphabetic(20);

    public SetupUtils() {
        this(System.getProperty("pravega.uri"));
    }

    public SetupUtils(String externalUri) {
        log.info("SetupUtils constructor");
        if (externalUri != null) {
            log.info("Using Pravega services at {}.", externalUri);
            gateway = new ExternalPravegaGateway(URI.create(externalUri));
        } else {
            log.info("Starting in-process Pravega services.");
            gateway = new InProcPravegaGateway();
        }
        log.info("Done with constructor {}", gateway.toString());
    }


    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        log.info("Starting all services");
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }
        log.info("Starting gateway");
        gateway.start();
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        try {
            gateway.stop();
        } catch (Exception e) {
            log.warn("Services did not stop cleanly (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Get resources as temp file.
     *
     * @param resourceName    Name of the resource.
     *
     * @return Path of the temp file.
     */
    static String getFileFromResource(String resourceName)  {
        try {
            Path tempPath = Files.createTempFile("test-", ".tmp");
            tempPath.toFile().deleteOnExit();
            try (InputStream stream = SetupUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
                Files.copy(SetupUtils.class.getClassLoader().getResourceAsStream(resourceName), tempPath, StandardCopyOption.REPLACE_EXISTING);
            }
            return tempPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Fetch the controller endpoint for this cluster.
     *
     * @return URI The controller endpoint to connect to this cluster.
     */
    public URI getControllerUri() {
        return getClientConfig().getControllerURI();
    }

    /**
     * Fetch the client configuration with which to connect to the controller.
     */
    public ClientConfig getClientConfig() {
        return this.gateway.getClientConfig();
    }

    /**
     * Create a controller facade for this cluster.
     * @return The controller facade, which must be closed by the caller.
     */
    public Controller newController() {
        ControllerImplConfig config = ControllerImplConfig.builder()
                .clientConfig(getClientConfig())
                .build();
        return new ControllerImpl(config, DEFAULT_SCHEDULED_EXECUTOR_SERVICE);
    }

    /**
     * Create a {@link EventStreamClientFactory} for this cluster and scope.
     */
    public EventStreamClientFactory newClientFactory() {
        return EventStreamClientFactory.withScope(this.scope, getClientConfig());
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(getClientConfig());
        streamManager.createScope(this.scope);
        streamManager.createStream(this.scope, streamName,
                StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(numSegments))
                        .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Get the stream.
     *
     * @param streamName     Name of the test stream.
     *
     * @return a Stream
     */
    public Stream getStream(final String streamName) {
        return Stream.of(this.scope, streamName);
    }

    /**
     * A gateway interface to Pravega for integration test purposes.
     */
    private interface PravegaGateway {
        /**
         * Starts the gateway.
         */
        void start() throws Exception;

        /**
         * Stops the gateway.
         */
        void stop() throws Exception;

        /**
         * Gets the client configuration with which to connect to the controller.
         */
        ClientConfig getClientConfig();
    }

    class InProcPravegaGateway implements PravegaGateway {

        // The pravega cluster.
        private InProcPravegaCluster inProcPravegaCluster = null;

        @Override
        public void start() throws Exception {
            log.info("Starting gateway");
            int zkPort = PortUtils.getAvailableListenPort();
            int controllerPort = PortUtils.getAvailableListenPort();
            int hostPort = PortUtils.getAvailableListenPort();
            int restPort = PortUtils.getAvailableListenPort();

            log.info("Building");
            this.inProcPravegaCluster = InProcPravegaCluster.builder()
                    .isInProcZK(true)
                    .secureZK(enableTls) //configure ZK for security
                    .zkUrl("localhost:" + zkPort)
                    .zkPort(zkPort)
                    .isInMemStorage(true)
                    .isInProcController(true)
                    .controllerCount(1)
                    .restServerPort(restPort)
                    .enableRestServer(enableRestServer)
                    .isInProcSegmentStore(true)
                    .segmentStoreCount(1)
                    .containerCount(4)
                    .enableMetrics(false)
                    .enableAuth(enableAuth)
                    .enableTls(enableTls)
                    //.certFile(getFileFromResource(CERT_FILE))   // pravega #2519
                    //.keyFile(getFileFromResource(KEY_FILE))
                    //.jksKeyFile(getFileFromResource(STANDALONE_KEYSTORE_FILE))
                    //.jksTrustFile(getFileFromResource(STANDALONE_TRUSTSTORE_FILE))
                    //.keyPasswordFile(getFileFromResource(STANDALONE_KEYSTORE_PASSWD_FILE))
                    //.passwdFile(getFileFromResource(PASSWD_FILE))
                    //.userName(PRAVEGA_USERNAME)
                    //.passwd(PRAVEGA_PASSWORD)
                    .build();
            log.info("Done building");
            this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
            this.inProcPravegaCluster.setSegmentStorePorts(new int[]{hostPort});
            this.inProcPravegaCluster.start();
            log.info("Initialized Pravega Cluster");
            log.info("Controller port is {}", controllerPort);
            log.info("Host port is {}", hostPort);
            log.info("REST server port is {}", restPort);
        }

        @Override
        public void stop() throws Exception {
            inProcPravegaCluster.close();
        }

        @Override
        public ClientConfig getClientConfig() {
            log.info("Getting client config");
            return ClientConfig.builder()
                    .controllerURI(URI.create(inProcPravegaCluster.getControllerURI()))
                    //.credentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                    //.validateHostName(enableHostNameValidation)
                    //.trustStore(getFileFromResource(CLIENT_TRUST_STORE_FILE))
                    .build();
        }
    }

    class ExternalPravegaGateway implements PravegaGateway {

        private final URI controllerUri;

        public ExternalPravegaGateway(URI controllerUri) {
            this.controllerUri = controllerUri;
        }

        @Override
        public void start() throws Exception {
        }

        @Override
        public void stop() throws Exception {
        }

        @Override
        public ClientConfig getClientConfig() {
            return ClientConfig.builder()
                    .controllerURI(controllerUri)
                    //.credentials(new DefaultCredentials(PRAVEGA_PASSWORD, PRAVEGA_USERNAME))
                    //.validateHostName(enableHostNameValidation)
                    //.trustStore(getFileFromResource(CLIENT_TRUST_STORE_FILE))
                    .build();
        }
    }

    static class PortUtils {
        // Linux uses ports from range 32768 - 61000.
        private static final int BASE_PORT = 32768;
        private static final int MAX_PORT_COUNT = 28232;
        private static final AtomicInteger NEXT_PORT = new AtomicInteger(1);

        /**
         * A helper method to get a random free port.
         *
         * @return free port.
         */
        public static int getAvailableListenPort() {
            for (int i = 0; i < MAX_PORT_COUNT; i++) {
                int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
                try {
                    ServerSocket serverSocket = new ServerSocket(candidatePort);
                    serverSocket.close();
                    return candidatePort;
                } catch (IOException e) {
                    // Do nothing. Try another port.
                }
            }
            throw new IllegalStateException(
                    String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
        }
    }
}
