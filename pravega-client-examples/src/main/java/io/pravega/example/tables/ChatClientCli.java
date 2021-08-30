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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Chat Application CLI (entry point. See {@link ChatApplication} for details, including data architecture.
 * <p>
 * How to use this:
 * - This is a command-line demo. The user types in instructions on the command line and they get executed.
 * - Syntax: {command-name} {arguments}
 * -- If {command-name} is prefixed by {@code @}, that will indicate it's a message to send to a user/channel.
 * -- Any other {command-name}s will be interpreted as actions. See below.
 * <p>
 * The following are a list of commands:
 * <p>
 * - exit: Exits the demo application.
 * - connect {Pravega Controller URI}: Connects to a Pravega Cluster. Default is {@link #DEFAULT_CONTROLLER_URI}.
 * - create-user {username}: Creates a new user with the given username.
 * - create-channel {channelname}: Creates a new Public Channel with the given name.
 * - list-users: List all the users in the system.
 * - list-channels: Lists all channels in the system.
 * - login: {username}: The user by the specified user name is now active in this session.
 * - subscribe {channelname}: Subscribes the currently logged in user to the given channel. Until unsubscribed, all messages
 * published to this channel will be displayed as soon as they are published.
 * - unsubscribe {channelname}: Unsubscribes the currently logged in user from the given channel.
 * - list-subscriptions: Lists all subscriptions the currently logged in user has.
 * <p>
 * Example:
 * {@code
 * create-user user1
 * create-user user2
 * create-channel ch1
 * list-users
 * list-channels
 * login user1
 * subscribe #ch1
 *
 * @user2 Hey user2! How are you?
 * @#ch1 Hey everyone. This is the first message ever on this channel.
 * login user2
 * @#ch1 Hey there. Everything is good.
 * login user1
 * exit
 * }
 * <p>
 * Multiple instances of {@link ChatClientCli} can be started, in different processes, or even on different machines,
 * as long as they connect to the same Pravega cluster.
 */
public final class ChatClientCli {
    private static final AtomicReference<ChatApplication> CHAT_CLIENT = new AtomicReference<>();
    private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";

    public static void main(String[] args) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega Chat Application Demo.");
        printHelp();
        try {
            executeCommand("connect", DEFAULT_CONTROLLER_URI);
            System.out.println();
            run();
        } finally {
            closeChatClient();
        }
        System.exit(0);
    }

    private static void printHelp() {
        System.out.println("Please refer to Javadoc for instructions and examples.\n");
    }

    private static void run() {
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            String line = input.nextLine();
            try {
                if (!processInput(line)) {
                    break;
                }
            } catch (IllegalArgumentException ex) {
                System.out.println("Invalid input: " + ex.getMessage());
                printHelp();
            } catch (Exception ex) {
                System.out.println("Error: " + ex.getMessage());
                printHelp();
            }
        }
    }

    private static ChatApplication getChatClient() {
        val c = CHAT_CLIENT.get();
        Preconditions.checkState(c != null, "No connection established yet.");
        return c;
    }

    private static boolean processInput(String line) {
        line = line.trim();
        if (line.length() == 0) {
            return true;
        }

        int commandDelimPos = line.indexOf(" ");
        String command;
        String arg;
        if (commandDelimPos <= 0) {
            command = line;
            arg = null;
        } else {
            command = line.substring(0, commandDelimPos).trim().toLowerCase();
            arg = line.substring(commandDelimPos).trim();
        }

        if (command.startsWith("@")) {
            command = command.substring(1);
            getChatClient().getUserSession().sendMessage(command, arg);
            return true;
        } else {
            return executeCommand(command, arg);
        }
    }

    private static boolean executeCommand(String command, String arg) {
        switch (command) {
            case "exit":
                return false;
            case "connect":
                val newClient = new ChatApplication(URI.create(arg));
                try {
                    newClient.connect();
                    closeChatClient();
                    CHAT_CLIENT.set(newClient);
                } catch (Exception ex) {
                    newClient.close();
                    throw ex;
                }
                break;
            case "login":
                getChatClient().login(arg);
                break;
            case "create-user":
                getChatClient().createUser(arg);
                break;
            case "create-channel":
                getChatClient().createPublicChannel(arg);
                break;
            case "subscribe":
                getChatClient().getUserSession().subscribe(arg);
                break;
            case "unsubscribe":
                getChatClient().getUserSession().unsubscribe(arg);
                break;
            case "list-subscriptions":
                getChatClient().getUserSession().listSubscriptions();
                break;
            case "list-channels":
                getChatClient().listAllChannels();
                break;
            case "list-users":
                getChatClient().listAllUsers();
                break;
            default:
                System.out.println(String.format("Unknown command '%s'.", command));
        }
        return true;
    }


    private static void closeChatClient() {
        val c = CHAT_CLIENT.getAndSet(null);
        if (c != null) {
            c.close();
        }
    }
}
