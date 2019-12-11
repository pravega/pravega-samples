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
package io.pravega.example.statesynchronizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.pravega.client.admin.StreamManager;

/*
 * A simple console-based "interpreter" to invoke simple commands against a SharedConfig object.
 */
public class SharedConfigCli implements AutoCloseable{
    private static final String DEFAULT_SCOPE = "example";
    private static final String DEFAULT_CONFIG_NAME = "someConfig";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    
    private static final String[] HELP_TEXT = {
            "Enter one of the following commands at the command line prompt:",
            "",
            "GET_ALL - prints out all of the properties in the Shared Config.",
            "GET {key} - print out the configuration property for the given key.",
            "PUT {key} , {value} - update the Shared Config with the given key/value pair.  Print out previous value (if it existed).",
            "PUT_IF_ABSENT {key} , {value} - update the Shared Config with the given key/value pair, only if the property is not already defined.",
            "REMOVE {key} [ , {currentValue}] - remove the given property from the Shared Config.  If {currentValue} is given, remove only if the property's current value matches {currentValue}..",
            "REPLACE {key} , {newValue} [ , {currentValue}] - update the value of the property.  If {currentValue} is given, update only if the property's current value matches {cuurentValue}.",
            "CLEAR - remove all the keys from the Shared Config.",
            "REFRESH - force an update from the Synchronized State.",
            "HELP - print out a list of commands.",
            "QUIT - terminate the program."
    };
    
    private final String configName; //corresponds to the stream name used by the synchronizer behind the shared config
    
    private final SynchronizerClientFactory clientFactory;
    private final StreamManager streamManager;
    private final SharedConfig<String, String> config;

    public SharedConfigCli(String scope, String configName, URI controllerURI) {
        this.configName = configName;
        
        this.clientFactory = SynchronizerClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
        this.streamManager = StreamManager.create(controllerURI);
        
        this.config = new SharedConfig<>(clientFactory, streamManager, scope, configName);
    }
    
    /*
     * Use the console to accept commands from the command line and execute the commands against the shared config
     */
    public void run() throws IOException{
        boolean done = false;
        
        outputHelp();
        
        while(!done){
            String commandLine = readLine("%s >", getPrompt()).trim();
            if (! commandLine.equals("")) {
                done = processCommand(commandLine);
            }
        }
    }
    
    private String getPrompt() {
        return configName;
    }
    
    /*
     * Indirection to deal with Eclipse console bug #122429
     */
    private String readLine(String format, Object... args) throws IOException {
        if (System.console() != null) {
            return System.console().readLine(format, args);
        }
        System.out.print(String.format(format, args));
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));
        return reader.readLine();
        
    }
    
    /*
     * The raw format of the command is COMMAND [parm [,parm]]
     */
    private boolean processCommand(String rawString) {
        boolean ret = false;
        final Scanner sc = new Scanner(rawString);
        final String command = sc.next();
        List<String> parms;
        if (sc.hasNextLine()) {
            final String[] rawParms = sc.nextLine().split(",");
            parms = Arrays.asList(rawParms);
            parms.replaceAll(String::trim);
        } else {
            parms = new ArrayList<String>();
        }
        
        switch(command.toUpperCase()) {
            case "GET_ALL" :
                doGetAll(parms);
                break;
            case "GET" :
                doGet(parms);
                break;
            case "PUT" :
                doPut(parms);
                break;
            case "PUT_IF_ABSENT" :
                doPutIfAbsent(parms);
                break;
            case "REMOVE" :
                doRemove(parms);
                break;
            case "REPLACE" :
                doReplace(parms);
                break;
            case "CLEAR" :
                doClear(parms);
                break;
            case "REFRESH" :
                doRefresh(parms);
                break;
            case "HELP" :
                doHelp(parms);
                break;
            case "QUIT" :
                ret = true;
                output("Exiting...%n");
                break;
            default :
                unknownCommand(command);
                break;
        }
        sc.close();
        return ret;
    }
    
    private void doGetAll(List<String> parms) {
        Map<String, String> properties = config.getProperties();
        output("Properties for SharedConfig: '%s' :%n", configName);
        properties.forEach((k,v) -> output("    '%s' -> '%s'%n", k, v));
        
        if (parms.size() > 0) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }
    
    private void doGet(List<String> parms) {
        try {
            final String key = parms.get(0);
            final String value = config.getProperty(key);
            if (value == null ) {
                warn("Property '%s' is undefined%n", key);
            } else {
                output("The value of property '%s' is '%s'%n", key, value);
            }
        } catch (IndexOutOfBoundsException e) {
            warn("Please enter a key to retrieve%n");
        }
        
        if (parms.size() > 1) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms.stream().skip(1).collect(Collectors.toList())));
        }
    }
    
    private void doPut(List<String> parms) {
        try {
            final String key = parms.get(0);
            final String value = parms.get(1);
            
            final String oldValue = config.putProperty(key, value);
            if (oldValue == null) {
                output("Property '%s' added with value '%s'%n", key, value);
            } else {
                output("Property '%s' updated from: '%s' to: '%s'%n", key, oldValue, value);
            }
        } catch (IndexOutOfBoundsException e) {
            warn("Expecting parameters: key , value %n");
        }
        
        if (parms.size() > 2) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms.stream().skip(2).collect(Collectors.toList())));
        }
    }
    
    private void doPutIfAbsent(List<String> parms) {
        try {
            final String key = parms.get(0);
            final String value = parms.get(1);
            final String oldValue = config.putPropertyIfAbsent(key, value);
            if (oldValue == null) {
                output("Property '%s' added with value '%s'%n", key, value);
            } else {
                output("Property '%s' exists with value '%s'%n", key, oldValue);
            }
        } catch (IndexOutOfBoundsException e) {
            warn("Expecting parameters: key , value %n");
        }
        
        if (parms.size() > 2) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms.stream().skip(2).collect(Collectors.toList())));
        }
    }
    
    private void doRemove(List<String> parms) {
        try {
            final String key = parms.get(0);
            if (parms.size() > 1) {
                final String currValue = parms.get(1);
                final boolean removed = config.removeProperty(key, currValue);
                if (removed) {
                    output("Property '%s' is removed -- old value was '%s'%n", key, currValue);
                } else {
                    warn("Property '%s' was NOT removed -- current value did not match given value '%s'%n", key, currValue);
                }
            } else {
                final String currValue = config.removeProperty(key);
                if (currValue == null ) {
                    output("Property '%s' is undefined; nothing to remove%n", key);
                } else {
                    output("Property for '%s' is removed -- old value was '%s'%n", key, currValue);
                }
            }
        } catch (IndexOutOfBoundsException e) {
            warn("Expecting parameters: key [, currentValue]%n");
        }
        
        if (parms.size() > 2) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms.stream().skip(2).collect(Collectors.toList())));
        }
    }
    
    private void doReplace(List<String> parms) {
        try {
            final String key = parms.get(0);
            final String newValue = parms.get(1);
            if (parms.size() > 2) {
                final String currValue = parms.get(2);
                final boolean replacmentMade = config.replaceProperty(key, currValue, newValue);
                if (replacmentMade) {
                    output("Property '%s' had value '%s', now replaced with new value '%s'%n", key, currValue, newValue);
                } else {
                    warn("Property '%s' was NOT replaced -- current value did not match given value '%s'%n", key, currValue);
                }
            } else {
                final String oldValue = config.replaceProperty(key, newValue);
                if (oldValue == null) {
                    output("Property '%s' is undefined%n", key);
                } else {
                    output("Property '%s' had value '%s', now replaced with new value '%s'%n", key, oldValue, newValue);
                }
            }
        } catch (IndexOutOfBoundsException e) {
            warn("Expecting parameters: key , newValue [, currentValue]%n");
        }
            
        if (parms.size() > 3) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms.stream().skip(3).collect(Collectors.toList())));
        }
    }
    
    private void doClear(List<String> parms) {
        config.clear();
        output("All properties removed%n");
        
        if (parms.size() > 0) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }
    
    private void doRefresh(List<String> parms) {
        config.synchronize();
        output("Properties refreshed%n");
        
        if (parms.size() > 0) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }
    
    private void doHelp(List<String> parms){
        outputHelp();
        
        if (parms.size() > 0) {
            warn("Ignoring parameters: '%s'%n", String.join(",", parms));
        }
    }
    
    private void outputHelp() {
        Arrays.stream(HELP_TEXT).forEach(System.out::println);
        System.out.println("");
    }
    
    private void unknownCommand(String command){
        output("Unknown command: '%s'%n", command);
        outputHelp();
    }
    
    private void output(String format, Object... args){
        System.out.format("**** ");
        System.out.format(format, args);
    }
    
    private void warn(String format, Object... args){
        System.out.format("!!!! ");
        System.out.format(format, args);
    }
    
    private void output(Exception e) {
        e.printStackTrace();
        output("%n");
    }
    
    @Override
    public void close(){
        if (config != null) {
            config.close();
        }
        
        if (streamManager != null) {
            streamManager.close();
        }
        
        if (clientFactory != null) {
            clientFactory.close();
        }
    }
    
    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope (namespace) of the Shared Config.");
        options.addOption("n", "name", true, "The name of the Shared Config.");
        options.addOption("u", "uri", true, "The URI to the Pravega controller in the form tcp://host:port");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
    
    public static void main(String[] args) {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SharedConfigCLi", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String configName = cmd.getOptionValue("name") == null ? DEFAULT_CONFIG_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        
        final URI controllerURI = URI.create(uriString);
        
        try(
            SharedConfigCli cli = new SharedConfigCli(scope, configName, controllerURI);
           ){
            cli.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
