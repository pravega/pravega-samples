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
package io.pravega.example.tables;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.Insert;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableKey;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

/**
 * This implements a simple Chat Application using Pravega Streams, StreamCuts and Key-Value Tables. All the logic for the
 * Chat Application lies within this class, and Pravega is used solely for data storage (there is no "Chat Service" that
 * this application talks to).
 * <p>
 * <p/>
 * In this implementation:
 * <ul>
 * <li>Streams are used to record messages sent in conversations.
 * <li>Stream Cuts are used to record how far has a user "read" a conversation.
 * <li>Key-Value Tables are used to hold 1) metadata about conversations, 2) users and 3) what conversations a user is
 * subscribed to (as well as the position (Stream Cut) within those conversations).
 * <li>When a user "logs in", their conversation subscriptions are read from the Key-Value Table and the latest read message.
 * {@link EventStreamReader}s (Subscription listeners) are used to tail the appropriate conversations after that point.
 * </ul>
 * <p>
 * Chat Service architecture:
 * <ul>
 * <li>There is a set of Users with unique user names. The users are stored in a Key-Value Table, keyed by username.
 * <li>There is a set of Public Channels with unique names. The Channels are stored in a Key-Value Table, keyed by
 * channel name.
 * <li>A Conversation is either a Public Channel or a direct-message conversation (between two users).
 * <li>Each Conversation (whether direct or Public Channel) is backed by a single Pravega Stream (1:1 mapping).
 * <li>User-Channel Subscriptions are stored in a Key-Value Table, keyed by "Username-Channelname".
 * </ul>
 * <p>
 * See {@link ChatClientCli} for instructions on how to run.
 */
@SuppressWarnings("UnstableApiUsage")
final class ChatApplication implements AutoCloseable {
    //region Members

    /**
     * Scope to use in Pravega cluster. All Streams and Key-Value Tables will be created under this Scope.
     */
    private static final String CHAT_SCOPE = "PravegaChatApplicationDemo";
    /**
     * Key-Value Table name for Users.
     */
    private static final String USER_TABLE_NAME = "Users";
    /**
     * Key-Value Table name for Channels.
     */
    private static final String CHANNEL_TABLE_NAME = "Channels";
    /**
     * Key-Value Table name for User-Channel subscriptions.
     */
    private static final String USER_SUBSCRIPTIONS_TABLE_NAME = "UserSubscriptions";
    /**
     * Usernames may be up to 64 latin characters and numbers.
     */
    private static final PaddedStringSerializer USERNAME_SERIALIZER = new PaddedStringSerializer(64);
    /**
     * Channel names may be up to 64 latin characters and numbers. To accommodate user-user channels, we allow double that
     * amount for keys.
     */
    private static final PaddedStringSerializer CHANNEL_NAME_SERIALIZER = new PaddedStringSerializer(USERNAME_SERIALIZER.getMaxLength() * 2);
    private static final UTF8StringSerializer STRING_SERIALIZER = new UTF8StringSerializer();

    private final URI controllerUri;
    private final StreamManager streamManager;
    private final KeyValueTableManager keyValueTableManager;
    @Getter
    private final EventStreamClientFactory clientFactory;
    @Getter
    private final ReaderGroupManager readerGroupManager;
    private final KeyValueTableFactory keyValueTableFactory;
    @Getter
    private final ScheduledExecutorService executor;
    private KeyValueTable userTable;
    private KeyValueTable userSubscriptionTable;
    private KeyValueTable channelTable;
    private User userSession;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link ChatApplication} class.
     *
     * @param controllerUri A {@link URI} pointing to the Pravega Controller to connect to.
     */
    ChatApplication(@NonNull URI controllerUri) {
        this.controllerUri = controllerUri;
        this.streamManager = StreamManager.create(controllerUri);
        this.keyValueTableManager = KeyValueTableManager.create(controllerUri);
        val clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
        this.clientFactory = EventStreamClientFactory.withScope(CHAT_SCOPE, clientConfig);
        this.readerGroupManager = ReaderGroupManager.withScope(CHAT_SCOPE, clientConfig);
        this.keyValueTableFactory = KeyValueTableFactory.withScope(CHAT_SCOPE, clientConfig);
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "chat-client");
    }

    @Override
    public void close() {
        close(this.userSession);
        close(this.userTable);
        close(this.userSubscriptionTable);
        close(this.channelTable);

        this.streamManager.close();
        this.readerGroupManager.close();
        this.keyValueTableManager.close();
        this.keyValueTableFactory.close();
        this.clientFactory.close();
        ExecutorServiceHelpers.shutdown(this.executor);
        System.out.println(String.format("Disconnected from '%s'.", this.controllerUri));
    }

    @SneakyThrows
    private void close(AutoCloseable c) {
        if (c != null) {
            c.close();
        }
    }

    //endregion

    //region Helpers

    private static String getChannelName(String name) {
        return isPublicChannelName(name) ? name : "#" + name;
    }

    private static boolean isPublicChannelName(String name) {
        return name.startsWith("#");
    }

    private static String getChannelScopedStreamName(String channelName) {
        return NameUtils.getScopedStreamName(CHAT_SCOPE, getChannelStreamName(channelName));
    }

    private static String getChannelStreamName(String channelName) {
        return isPublicChannelName(channelName) ? channelName.substring(1) : channelName;
    }

    private static void reportError(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        if (ex instanceof InterruptedException) {
            return;
        }
        ex.printStackTrace();
    }

    private static TableKey toKey(String s, PaddedStringSerializer serializer) {
        return new TableKey(serializer.serialize(s));
    }

    //endregion

    //region API

    /**
     * Attempts to establish a connection to the Pravega Controller and create necessary Scope(s) and Key-Value Tables.
     */
    void connect() {
        System.out.println(String.format("Connecting to '%s' ...", this.controllerUri));
        val scopeCreated = this.streamManager.createScope(CHAT_SCOPE);
        if (scopeCreated) {
            System.out.println(String.format("\tScope '%s' created. There are no users or channels registered yet.", CHAT_SCOPE));
        } else {
            System.out.println(String.format("\tScope '%s' already exists. There may already be users or channels registered.", CHAT_SCOPE));
        }

        // Create all the tables.
        // The User and Channel Tables only have Primary Keys - everything is keyed off their names.
        this.userTable = createTable(USER_TABLE_NAME, 2, USERNAME_SERIALIZER.getMaxLength(), 0);
        this.channelTable = createTable(CHANNEL_TABLE_NAME, 2, CHANNEL_NAME_SERIALIZER.getMaxLength(), 0);

        // The Subscription Table has a Primary Key (the username) and Secondary Keys (the channels the user is subscribed to).
        this.userSubscriptionTable = createTable(USER_SUBSCRIPTIONS_TABLE_NAME, 4,
                USERNAME_SERIALIZER.getMaxLength(), CHANNEL_NAME_SERIALIZER.getMaxLength());

        System.out.println(String.format("Connected to '%s'.", this.controllerUri));
    }

    private KeyValueTable createTable(String tableName, int partitionCount, int pkLength, int skLength) {
        val kvtConfig = KeyValueTableConfiguration.builder()
                .partitionCount(partitionCount)
                .primaryKeyLength(pkLength)
                .secondaryKeyLength(skLength)
                .build();
        val kvtCreated = this.keyValueTableManager.createKeyValueTable(CHAT_SCOPE, tableName, kvtConfig);
        if (kvtCreated) {
            System.out.println(String.format("\tTable '%s/%s' created.", CHAT_SCOPE, tableName));
        } else {
            System.out.println(String.format("\tTable '%s/%s' already exists.", CHAT_SCOPE, tableName));
        }

        return this.keyValueTableFactory.forKeyValueTable(tableName, KeyValueTableClientConfiguration.builder().build());
    }

    /**
     * Returns the currently logged-in {@link User} session.
     *
     * @return The user session.
     */
    User getUserSession() {
        ensureConnected();
        Preconditions.checkArgument(this.userSession != null, "No user logged in yet.");
        return this.userSession;
    }

    /**
     * Publishes a message by appending (writing) an Event to the Stream associated with the given channel.
     *
     * @param channelName  The name of the channel to publish.
     * @param fromUserName The username of the user that publishes the message.
     * @param message      The message to publish.
     */
    void publish(String channelName, String fromUserName, String message) {
        validateUserName(fromUserName);
        val channelStreamName = getChannelStreamName(channelName);
        validateChannelName(channelStreamName);
        try (val w = this.clientFactory.createEventWriter(channelStreamName, new UTF8StringSerializer(), EventWriterConfig.builder().build())) {
            w.writeEvent(fromUserName, message);
        }
    }

    /**
     * Logs in a user by retrieving that user's state from the User Key-Value Table and establishes that {@link User}'s
     * session. If another user is already logged in, that user will be logged out.
     *
     * @param userName The username of the user to login.
     */
    void login(String userName) {
        validateUserName(userName);
        ensureConnected();
        val userData = this.userTable.get(toKey(userName, USERNAME_SERIALIZER)).join();
        if (userData == null) {
            System.out.println(String.format("No user with id '%s' is registered.", userName));
            return;
        }

        close(this.userSession);
        this.userSession = new User(userName);
        System.out.println(String.format("User session for '%s' started.", userName));
        getUserSession().startListening();
    }

    /**
     * Creates a new user by (conditionally) inserting an entry into the User Key-Value Table.
     *
     * @param userName The username of the user to create.
     */
    void createUser(String userName) {
        validateUserName(userName);
        ensureConnected();
        val newData = Instant.now().toString();
        val insert = new Insert(toKey(userName, USERNAME_SERIALIZER), STRING_SERIALIZER.serialize(newData));
        this.userTable.update(insert)
                .handle((r, ex) -> {
                    if (ex == null) {
                        System.out.println(String.format("User '%s' created successfully.", userName));
                    } else {
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof ConditionalTableUpdateException) {
                            System.out.println(String.format("User '%s' already exists.", userName));
                        } else {
                            throw new CompletionException(ex);
                        }
                    }
                    return null;
                }).join();
    }

    /**
     * Creates a new (public) channel by:
     * 1. Conditionally inserting a new entry into the Channel Key-Value Table.
     * 2. (If the above is successful) Creating a new Stream associated with the given Channel.
     *
     * @param channelName The name of the channel to create.
     */
    void createPublicChannel(String channelName) {
        validateChannelName(channelName);
        ensureConnected();
        String channelStreamName = NameUtils.validateStreamName(channelName);
        val actualChannelName = getChannelName(channelStreamName); // Make sure it has the proper prefix.
        val newData = Instant.now().toString();
        val insert = new Insert(toKey(actualChannelName, CHANNEL_NAME_SERIALIZER), STRING_SERIALIZER.serialize(newData));
        this.channelTable.update(insert)
                .thenCompose(v -> {
                    boolean streamCreated = this.streamManager.createStream(CHAT_SCOPE, channelStreamName,
                            StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(4)).build());
                    if (streamCreated) {
                        System.out.println(String.format("Channel '%s' created successfully. Stream name: '%s/%s'.", channelName,
                                CHAT_SCOPE, channelStreamName));
                        return CompletableFuture.completedFuture(null);
                    } else {
                        System.out.println(String.format("Unable to create Stream '%s/%s' for channel '%s'.", CHAT_SCOPE,
                                channelStreamName, channelName));
                        return this.channelTable.update(new Remove(toKey(channelName, CHANNEL_NAME_SERIALIZER)));
                    }
                })
                .handle((r, ex) -> {
                    if (ex != null) {
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof ConditionalTableUpdateException) {
                            System.out.println(String.format("Channel '%s' already exists.", channelName));
                        } else {
                            throw new CompletionException(ex);
                        }
                    }
                    return null;
                }).join();
    }

    /**
     * Creates a new (private/direct-message) channel by:
     * 1. Conditionally inserting a new entry into the Channel Key-Value Table.
     * 2. (If the above is successful) Creating a new Stream associated with the given Channel.
     *
     * @param channelName The name of the channel to create.
     */
    void createDirectMessageChannel(String channelName) {
        validateChannelName(channelName);
        ensureConnected();
        String channelStreamName = NameUtils.validateStreamName(channelName);
        val newData = Instant.now().toString();
        val insert = new Insert(toKey(channelName, CHANNEL_NAME_SERIALIZER), STRING_SERIALIZER.serialize(newData));

        this.channelTable.update(insert)
                .handle((r, ex) -> {
                    if (ex == null) {
                        // Create a stream for this channel.
                        this.streamManager.createStream(CHAT_SCOPE, channelStreamName,
                                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(4)).build());
                        System.out.println(String.format("Created new direct channel '%s'.", channelName));
                    } else if (!(Exceptions.unwrap(ex) instanceof ConditionalTableUpdateException)) {
                        throw new CompletionException(ex);
                    }
                    return null;
                });

    }

    /**
     * Lists all created (public) channels by performing a Key Iterator over the Channel Key-Value Table.
     */
    void listAllChannels() {
        ensureConnected();
        val count = new AtomicInteger(0);
        this.channelTable.iterator()
                .maxIterationSize(10)
                .all()
                .keys()
                .forEachRemaining(ii -> {
                    for (val channel : ii.getItems()) {
                        val channelName = CHANNEL_NAME_SERIALIZER.deserialize(channel.getPrimaryKey());
                        if (isPublicChannelName(channelName)) {
                            System.out.println(String.format("\t%s", channelName));
                            count.incrementAndGet();
                        }
                    }
                }, this.executor).join();
        System.out.println(String.format("Total channel count: %s.", count));
    }

    /**
     * Lists all registered users by performing a Key Iterator over the User Key-Value Table.
     */
    void listAllUsers() {
        ensureConnected();
        val count = new AtomicInteger(0);
        this.userTable.iterator()
                .maxIterationSize(10)
                .all()
                .keys()
                .forEachRemaining(ii -> {
                    for (val user : ii.getItems()) {
                        val userName = USERNAME_SERIALIZER.deserialize(user.getPrimaryKey());
                        System.out.println(String.format("\t%s", userName));
                        count.incrementAndGet();
                    }
                }, this.executor).join();
        System.out.println(String.format("Total user count: %s.", count));
    }

    private void validateChannelName(String channelName) {
        Exceptions.checkNotNullOrEmpty(channelName, "channelName");
        Preconditions.checkArgument(channelName.length() > 0 && channelName.length() <= CHANNEL_NAME_SERIALIZER.getMaxLength(),
                "Channel Name must be non-empty and shorter than %s.", CHANNEL_NAME_SERIALIZER.getMaxLength());
        NameUtils.validateStreamName(channelName); // Checks for invalid characters.
    }

    private void validateUserName(String userName) {
        Exceptions.checkNotNullOrEmpty(userName, "userName");
        Preconditions.checkArgument(userName.length() > 0 && userName.length() <= USERNAME_SERIALIZER.getMaxLength(),
                "UserName must be non-empty and shorter than %s.", USERNAME_SERIALIZER.getMaxLength());
        NameUtils.validateUserKeyValueTableName(userName); // Checks for invalid characters.
    }

    private void ensureConnected() {
        Preconditions.checkState(this.userTable != null, "Not connected.");
    }

    //endregion

    //region User

    /**
     * User Session.
     */
    @Data
    class User implements AutoCloseable {
        private final String userName;
        private final AtomicReference<ScheduledFuture<?>> subscriptionListener = new AtomicReference<>();
        private final AtomicReference<Map<String, SubscriptionListener>> currentSubscriptions = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Subscribes the current user to a channel.
         *
         * @param channelName The name of the channel to subscribe to.
         */
        void subscribe(String channelName) {
            channelName = getChannelName(channelName);
            Preconditions.checkArgument(channelTable.exists(toKey(channelName, CHANNEL_NAME_SERIALIZER)).join(),
                    "Channel '%s' does not exist.", channelName);
            boolean subscribed = subscribeUserToChannel(userName, channelName);
            if (subscribed) {
                System.out.println(String.format("User '%s' has been subscribed to '%s'.", this.userName, channelName));
            } else {
                System.out.println(String.format("User '%s' is already subscribed to '%s'.", this.userName, channelName));
            }
        }

        /**
         * Unsubscribes the current user from a channel, by removing the Entry keyed by the current user's username and
         * the given channel from the Channel Key-Value Table.
         *
         * @param channelName The name of the channel to unsubscribe from.
         */
        void unsubscribe(String channelName) {
            Exceptions.checkNotNullOrEmpty(channelName, "channelName");
            val key = new TableKey(USERNAME_SERIALIZER.serialize(userName), CHANNEL_NAME_SERIALIZER.serialize(channelName));
            userSubscriptionTable.update(new Remove(key));
            System.out.println(String.format("User '%s' has been unsubscribed from '%s'.", this.userName, channelName));
        }

        /**
         * Sends a message.
         * @param target The target channel name.
         * @param message The message.
         */
        void sendMessage(String target, String message) {
            String channelName = target;
            if (!target.startsWith("#")) {
                // Direct user message.
                channelName = Stream.of(target, this.userName).sorted().collect(Collectors.joining("-"));

                // Create a chat client, if needed.
                createDirectMessageChannel(channelName);

                // Subscribe this user to this channel, if necessary.
                subscribeUserToChannel(userName, channelName);

                // Subscribe the other user to this channel, if necessary.
                subscribeUserToChannel(target, channelName);
            }

            // Publish the message.
            message = String.format("[%s] %s", this.userName, message);
            publish(channelName, this.userName, message);
        }

        /**
         * Lists all subscriptions for this user.
         */
        void listSubscriptions() {
            val subscriptions = this.currentSubscriptions.get();
            if (subscriptions == null || subscriptions.isEmpty()) {
                System.out.println("No subscriptions.");
                return;
            }

            System.out.println(String.format("Subscription count: %s.", subscriptions.size()));
            subscriptions.keySet().forEach(channelName -> System.out.println(String.format("\t%s", channelName)));
        }

        /**
         * Initiates listening to changes to the User-Channel Subscription table for new/updated subscriptions.
         */
        void startListening() {
            Preconditions.checkState(this.subscriptionListener.get() == null, "Already listening.");

            // Listen to all subscribed channels.
            // Periodically list all keys in UserSubscriptions-{Session.userName} to detect new channels.
            this.subscriptionListener.set(executor.scheduleWithFixedDelay(this::refreshSubscriptionList, 1000, 1000, TimeUnit.MILLISECONDS));
            System.out.println(String.format("Listening to all subscriptions for user '%s'.", this.userName));
        }

        /**
         * Subscribes the given user to the given channel by (conditionally) inserting an entry into the User-Channel
         * subscription Key-Value Table.
         * @param userName The name of the user to subscribe.
         * @param channelName The channel name to subscribe the user to.
         * @return True if the subscription was successful, false otherwise (i.e., the user was already subscribed).
         */
        private boolean subscribeUserToChannel(String userName, String channelName) {
            val key = new TableKey(USERNAME_SERIALIZER.serialize(userName), CHANNEL_NAME_SERIALIZER.serialize(channelName));
            val insert = new Insert(key, STRING_SERIALIZER.serialize(StreamCut.UNBOUNDED.asText()));
            return Futures.exceptionallyExpecting(
                    userSubscriptionTable.update(insert).thenApply(v -> true),
                    ex -> ex instanceof ConditionalTableUpdateException, // If this user is already subscribed, the update will be rejected.
                    false)
                    .join();
        }

        /**
         * Refreshes the subscription list for the given user by performing a Primary-Key Key Iterator on the User-Channel
         * Subscription Table (for all Keys where Primary Key matches the current user).
         */
        private void refreshSubscriptionList() {
            val currentSubscribedChannels = new HashSet<String>();
            userSubscriptionTable.iterator()
                    .maxIterationSize(10)
                    .forPrimaryKey(USERNAME_SERIALIZER.serialize(this.userName))
                    .keys()
                    .forEachRemaining(ii -> {
                        for (val i : ii.getItems()) {
                            currentSubscribedChannels.add(CHANNEL_NAME_SERIALIZER.deserialize(i.getSecondaryKey()));
                        }
                    }, executor).join();

            val existing = this.currentSubscriptions.get() == null
                    ? new HashMap<String, SubscriptionListener>()
                    : new HashMap<>(this.currentSubscriptions.get());

            // Remove the ones we have since unsubscribed from.
            val removedSubscriptions = Sets.difference(existing.keySet(), currentSubscribedChannels);
            removedSubscriptions.forEach(channelName -> existing.remove(channelName).close());

            // Add new subscriptions.
            val newSubscriptions = Sets.difference(currentSubscribedChannels, existing.keySet());
            newSubscriptions.forEach(channelName -> {
                try {
                    existing.put(channelName, new SubscriptionListener(channelName, this.userName));
                } catch (Exception ex) {
                    if (!this.closed.get()) {
                        System.out.println(String.format("Error: unable to start a listener for '%s': %s.", channelName, ex.toString()));
                        reportError(ex);
                    }
                }
            });
            this.currentSubscriptions.set(existing);
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                val sl = this.subscriptionListener.getAndSet(null);
                if (sl != null) {
                    sl.cancel(true);
                }

                val subscriptions = this.currentSubscriptions.getAndSet(null);
                if (subscriptions != null) {
                    subscriptions.values().forEach(SubscriptionListener::close);
                }

                System.out.println(String.format("User session for '%s' ended.", this.userName));
            }
        }
    }

    //endregion

    //region SubscriptionListener

    private class SubscriptionListener implements AutoCloseable {
        private final String channelName;
        private final String userName;
        private final EventStreamReader<String> reader;
        private final String readerGroupId;
        private final ScheduledFuture<?> readPoll;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        SubscriptionListener(@NonNull String channelName, @NonNull String userName) {
            this.channelName = channelName;
            this.userName = userName;

            val positionText = userSubscriptionTable.get(getSubscriptionKey()).join();
            val position = positionText == null ? StreamCut.UNBOUNDED : StreamCut.from(STRING_SERIALIZER.deserialize(positionText.getValue()));

            this.readerGroupId = UUID.randomUUID().toString().replace("-", "");
            val readerId = UUID.randomUUID().toString().replace("-", "");
            val rgConfig = ReaderGroupConfig.builder().stream(getChannelScopedStreamName(this.channelName), position).build();
            getReaderGroupManager().createReaderGroup(this.readerGroupId, rgConfig);
            this.reader = getClientFactory().createReader(readerId, this.readerGroupId, new UTF8StringSerializer(),
                    ReaderConfig.builder().build());

            // Stream cut points to the last event we read, so we don't want to display it again.
            if (readOnce() == 0) {
                System.out.println(String.format("You're all caught up on '%s'.", this.channelName));
            }

            this.readPoll = executor.scheduleWithFixedDelay(this::readOnce, 100, 100, TimeUnit.MILLISECONDS);
        }

        private TableKey getSubscriptionKey() {
            return new TableKey(USERNAME_SERIALIZER.serialize(userName), CHANNEL_NAME_SERIALIZER.serialize(channelName));
        }

        private int readOnce() {
            try {
                //StreamCut lastStreamCut = null;
                int eventCount = 0;
                while (true) {
                    // Begin recording a Stream Cut. Whenever we are done we can record the position as having been read
                    // up to here.
                    beginStreamCut().whenComplete((streamCuts, ex) -> {
                        if (this.closed.get()) {
                            return;
                        }

                        if (ex != null) {
                            reportError(ex);
                        } else {
                            recordStreamCut(streamCuts);
                        }
                    });
                    EventRead<String> e = this.reader.readNextEvent(1000);
                    if (e.getEvent() == null) {
                        break;
                    }

                    System.out.println(String.format("> %s: %s", this.channelName, e.getEvent()));
                    eventCount++;
                }
                return eventCount;
            } catch (Exception ex) {
                if (!this.closed.get()) {
                    reportError(ex);
                }
                return -1;
            }
        }

        private CompletableFuture<Map<io.pravega.client.stream.Stream, StreamCut>> beginStreamCut() {
            return getReaderGroupManager().getReaderGroup(this.readerGroupId).generateStreamCuts(executor);
        }

        private void recordStreamCut(Map<io.pravega.client.stream.Stream, StreamCut> streamCuts) {
            val lastStreamCut = streamCuts.values().stream().findFirst().orElse(null);
            val update = new Put(getSubscriptionKey(), STRING_SERIALIZER.serialize(lastStreamCut.asText()));
            userSubscriptionTable.update(update).join();
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                this.readPoll.cancel(true);
                this.reader.close();
                getReaderGroupManager().deleteReaderGroup(this.readerGroupId);
                System.out.println(String.format("Stopped listening on '%s'.", this.channelName));
            }
        }
    }

    //endregion

    //region Helper Classes

    @Data
    private static class PaddedStringSerializer {
        private final int maxLength;

        ByteBuffer serialize(String s) {
            Preconditions.checkArgument(s.length() <= maxLength);
            s = Strings.padStart(s, this.maxLength, ' ');
            return ByteBuffer.wrap(s.getBytes(StandardCharsets.US_ASCII));
        }

        String deserialize(ByteBuffer b) {
            Preconditions.checkArgument(b.remaining() <= maxLength);
            String s = StandardCharsets.US_ASCII.decode(b).toString();
            s = s.trim();
            return s;
        }

    }

    //endregion
}
