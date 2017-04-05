package com.emc.pravega.example.statesynchronizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;

/*
 * A simple console-based "interpreter" to invoke simple commands against a SharedConfig object.
 */
public class SharedConfigCli implements AutoCloseable{
    private static final String DEFAULT_SCOPE = "some_scope";
    private static final String DEFAULT_CONFIG_NAME = "some_config";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    
    private static final String[] HELP_TEXT = {
            "Enter one of the following commands at the command line prompt:",
            "",
            "GET_ALL - prints out all of the properties in the Shared Config.",
            "GET {key} - print out the configuration property for the given key.",
            "PUT {key} , {value} - update the Shared Config with the given key/value pair.  Print out previous value (if it existed).",
            "REMOVE {key} - remove the given property from the Shared Config.  Print out the old value (if it existed).",
            "CLEAR - remove all the keys from the Shared Config.",
            "REFRESH - force an update from the Synchronized State.",
            "HELP - print out a list of commands.",
            "QUIT - terminate the program."
    };
    
    private final String configName; //corresponds to the stream name used by the synchronizer behind the shared config
    
    private final ClientFactory clientFactory;
    private final StreamManager streamManager;
    private final SharedConfig<String, String> config;

    public SharedConfigCli(String scope, String configName, URI controllerURI) {
        this.configName = configName;
        
        this.clientFactory = ClientFactory.withScope(scope, controllerURI);
        this.streamManager = StreamManager.create(controllerURI);
        
        this.config = new SharedConfig<>(clientFactory, streamManager, scope, configName);
    }
    
    /*
     * Use the console to accept commands from the command line and execute the commands against the shared config
     */
    public void run() throws IOException{
        boolean done = false;
        
        doHelp();
        
        while(!done){
            String commandLine = readLine("%s >", configName);
            done = processCommand(commandLine);
        }
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
        switch(command.toUpperCase()) {
            case "GET_ALL" :
                doGetAll();
                break;
            case "GET" :
                doGet(sc);
                break;
            case "PUT" :
                doPut(sc);
                break;
            case "REMOVE" :
                doRemove(sc);
                break;
            case "CLEAR" :
                doClear();
                break;
            case "REFRESH" :
                doRefresh();
                break;
            case "HELP" :
                doHelp();
                break;
            case "QUIT" :
                ret = true;
                break;
            default :
                unknownCommand(command);
                break;
        }
        sc.close();
        return ret;
    }
    
    private void doGetAll() {
        Map<String, String> properties = config.getProperties();
        output("Properties for SharedConfig: '%s' :%n", configName);
        properties.forEach((k,v) -> output("    '%s' -> '%s'%n", k, v));
    }
    
    private void doGet(Scanner sc) {
        if (sc.hasNext()) {
            String key = sc.next();
            String value = config.getProperty(key);
            if (value == null ) {
                output("Property '%s' is undefined%n", key);
            } else {
                output("The value of property '%s' is '%s'%n", key, value);
            }
        } else {
            output("Please enter a key to retrieve%n");
        }
    }
    
    private void doPut(Scanner sc) {
        if (sc.hasNext()) {
            String key = sc.next();
            if (sc.hasNext()) {
                String comma = sc.next();
                if (comma.equals(",")) {
                    if (sc.hasNext()) {
                        String value = sc.next();
                        String oldValue = config.putProperty(key, value);
                        if (oldValue == null) {
                            output("Property '%s' added with value '%s'%n", key, value);
                        } else {
                            output("Property '%s' updated from: '%s' to: '%s'%n", key, oldValue, value);
                        }
                        return;
                    }
                }
            }
        }
        
        output("Expecting key , value (note the whitespace before the comma)%n");
        
    }
    
    private void doRemove(Scanner sc) {
        if (sc.hasNext()) {
            String key = sc.next();
            String value = config.removeProperty(key);
            if (value == null ) {
                output("Property '%s' is undefined; nothing to remove%n", key);
            } else {
                output("Property for '%s' is removed -- old value was '%s'%n", key, value);
            }
        } else {
            output("Please enter a key to remove%n");
        }
    }
    
    private void doClear() {
        config.clear();
        output("All properties removed%n");
    }
    
    private void doRefresh() {
        config.synchronize();
        output("Properties refreshed%n");
    }
    
    private void doHelp(){
        Arrays.stream(HELP_TEXT).forEach(System.out::println);
        System.out.println("");
    }
    
    private void unknownCommand(String command){
        output("Unknown command: '%s'%n", command);
        doHelp();
    }
    
    private void output(String format, Object... args){
        System.out.format("**** ");
        System.out.format(format, args);
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
            e.printStackTrace();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldWriter", options);
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
