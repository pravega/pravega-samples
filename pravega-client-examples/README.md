# Pravega Client Example Applications
These applications only need a running Pravega to execute against.

## Pre requisites
1. Java 8
2. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)


## Publish Pravega jars to local Maven (optional)
If you have downloaded a nightly build of Pravega, you may need to generate the latest Pravega jar files and publish them to your local Maven repository.
For release builds of Pravega, the artifacts will already be in Maven Central and you won't need to run this step.

Note: maven 2 needs to be installed and running on your machine

In the root of Pravega (where Pravega's build.gradle file can be found), run:

```
$ ./gradlew install
```

The above command should generate the required jar files into your local maven repo.

## Generate the scripts to make it easier to run the examples
Most examples can be run either using the gradle wrapper (gradlew) or scripts.
To run the examples using scripts, the scripts need to be generated.  In the directory where you downloaded the pravega samples, run the following once, and all the scripts will be generated.

```
$ ./gradlew installDist
```

The scripts can be found under the pravega-samples directory in:

```
pravega-client-examples/build/install/pravega-client-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

## HelloPravega Example
This example consists of two applications, a HelloWorldReader that reads from a stream and a HelloWorldWriter, that writes to a stream.  You might want to run HelloWorldWriter in one window and HelloWorldReader in another window.

### HelloWorldWriter
A simple application that shows how to write to a Pravega stream.

```
$ bin/helloWorldWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090] [-routingkey myRK] [-message 'hello world']
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 * routingKey - "helloRoutingKey"
 * message - "hello world"

The program writes the given message with the given routing key to the stream with given scope/stream name.

### HelloWorldReader
A simple application that shows how to read from a Pravega stream.

```
$ bin/helloWorldReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

The program reads all the events from the stream with given scope/stream name and prints each event to the console.


## Console Reader and Writer Example
This example includes two applications, a ConsoleReader and a ConsoleWriter.  You might want to run ConsoleReader in one window and ConsoleWriter in another window.

### ConsoleReader
Use this application to launch an application that reads from a stream and emits all of the Events onto the console.  This application runs until you terminate it.

```
$ bin/consoleReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "someStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

### ConsoleWriter
Use this application to write to streams or transactions, and manage transactions.

The application uses the console to present an interactive DSL environment that presents operations to write events to
a stream or into a transaction.  In addition, it presents operations to begin, commit, abort, ping, check status on and
retrieve the id of a transaction.

```
$ bin/consoleWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "someStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

## State Synchronizer
This example illustrates the use of the Pravega StateSynchronizer.

The application implements a SharedMap object using StateSynchronizer.  We implement a SharedConfig object using
the SharedMap.  The SharedConfig simulates the idea of a properties configuration that needs to be kept in sync
across multiple processes.

To demonstrate manipulating the properties of the SharedConfig object, we provide a CLI.

```
$ bin/sharedConfigCli [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

Use the simple DSL to GET, PUT, REMOVE keys from the SharedConfig object identified by scope and name.

It is worthwhile to launch two or more separate CLIs in separate windows using the same settings and observe how changes in one
CLI process are not visible in another CLI process until that other CLI process invokes REFRESH.

## NoopReader

An example of a simple reader that continually reads the contents of any stream. A binary serializer is used so it works against any event types. The sample emits basic information about number of events/bytes read every 30 seconds. 

```
$ bin/noopReader [--uri tcp://127.0.0.1:9090] [--stream <SCOPE>/<STREAM>]
```
