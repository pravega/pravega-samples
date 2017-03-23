# Simple sample Pravega reader and writer.

## Pre requisites
1. download pravega from https://github.com/pravega/pravega
2. start the singleNode version by running

```
./gradlew startSingleNode
```

## HelloWorldReader
A simple application that shows how to read from a Pravega stream.

Run by 

```
./gradlew startHelloWorldReader -Dexec.args="-scope aScope -name aName -uri tcp://localhost:9090"
```

All args are optional, if not included, the defaults are:

 * scope - "hello_scope"
 * name - "hello_stream" 
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

The program reads all the events from the stream with given scope/stream name and prints each event to the console.

## HelloWorldWriter
A simple application that shows how to write to a Pravega stream.

Run by

```
./gradlew startHelloWorldWriter -Dexec.args="-scope aScope -name aName -uri tcp://localhost:9090 -routingKey someRK -message someMessage"
```

All args are optional, if not included, the defaults are:

 * scope - "hello_scope"
 * name - "hello_stream" 
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 * routingKey - "hello_routingKey"
 * message - "hello world"

The program writes the given message with the given routing key to the stream with given scope/stream name.
