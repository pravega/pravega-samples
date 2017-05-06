# Standalone Examples of Pravega Applications
These applications only need a running Pravega to execute against.

## Pre requisites
1. download pravega from https://github.com/pravega/pravega
2. start the singleNode version by running

```
$ ./gradlew startStandalone
```

## Publish Pravega jars to local Maven (optional)
If you have downloaded a nightly build of Pravega, you may need to generate the latest Pravega jar files and publish them to your local Maven repository.
For release builds of Pravega, the artifacts will already be in Maven Central and you won't need to run this step.

Note: maven 2 needs to be installed and running on your machine

In the root of Pravega (where Pravega's build.gradle file can be found), run:

```
$ ./gradlew install
```

The above command should generate the required jar files into your local maven repo.

## Generate the scripts to make it easier to run examples
Most examples can be run either using gradle wrapper (gradlew) or scripts.
To run the examples using scripts, the scripts need to be generated.  Run the following once, and all the scripts are generated

```
$ ./gradlew installDist
```

The scripts can be found under the pravega-samples directory in:

```
standalone-examples/build/install/pravega-standalone-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

Alternatively, you can run:

```
$ gradle distZip
```

to package the main distribution as a ZIP, or:

```
$ gradle distTar
```

to create a TAR file. To build both types of archives just run gradle assembleDist. The files will be created at `$buildDir/distributions/$project.name-$project.version.«ext»`.

## HelloPravega Example
This example consists of two applications, a HelloWorldReader that reads from a stream and a HelloWorldWriter, that writes to a stream.

### HelloWorldReader
A simple application that shows how to read from a Pravega stream.

Run using gradle wrapper:

```
$ ./gradlew startHelloWorldReader
```

or if you want don't want to take the defaults, use:

```
$ ./gradlew startHelloWorldReader -Dexec.args="-scope aScope -name aName -uri tcp://localhost:9090"
```

The example can also be executed by running the script (make sure you have followed the "Generate the Scripts..." step above).
```
$ cd standalone-examples/build/install/pravega-standalone-examples
$ ./bin/helloWorldReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

The program reads all the events from the stream with given scope/stream name and prints each event to the console.

### HelloWorldWriter
A simple application that shows how to write to a Pravega stream.

Run using gradle wrapper:

```
$ ./gradlew startHelloWorldWriter
```

or if you want don't want to take the defaults, use:

```
$ ./gradlew startHelloWorldWriter -Dexec.args="-scope aScope -name aName -uri tcp://localhost:9090 -routingKey someRK -message someMessage"
```

The example can also be executed by running the script (make sure you have followed the "Generate the Scripts..." step above).
```
$ cd standalone-examples/build/install/pravega-standalone-examples
$ ./bin/helloWorldWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090] [-routingkey myRK] [-message 'hello world']
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 * routingKey - "helloRoutingKey"
 * message - "hello world"

The program writes the given message with the given routing key to the stream with given scope/stream name.

## Console Reader and Writer Example
This example includes two applications, a ConsoleReader and a ConsoleWriter.

### ConsoleReader
Use this application to launch an application that reads from a stream and emits all of the Events onto the console.

Run using gradle wrapper:

```
$ ./gradlew startConsoleReader
```

or if you want don't want to take the defaults, use:

```
$ ./gradlew startConsoleReader -Dexec.args="-scope aScope -name aName -uri tcp://localhost:9090"
```

The example can also be executed by running the script (make sure you have followed the "Generate the Scripts..." step above).
```
$ cd standalone-examples/build/install/pravega-standalone-examples
$ ./bin/consoleReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
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

ConsoleWriter MUST be run using the scripts.  Gradle interferes with console I/O.  Make sure you have followed the "Generate the Scripts..." step above.

```
$ cd standalone-examples/build/install/pravega-standalone-examples
$ ./bin/consoleWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
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

The CLI MUST be run using the scripts.  Gradle interferes with console I/O.  Make sure you have followed the "Generate the Scripts..." step above.

```
$ cd standalone-examples/build/install/pravega-standalone-examples
$ ./bin/sharedConfigCli [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

Use the simple DSL to GET, PUT, REMOVE keys from the SharedConfig object identified by scope and name.

It is worthwhile to launch two or more separate CLIs in several terminal windows using the same settings and observe how changes in one
CLI process is not visible in another CLI process until that other CLI process invokes REFRESH.

## TurbineHeatSensor

An example of a lightweight IOT application that writes simulated sensor events to a Pravega stream.

To run from the IDE:

```
./gradlew startTurbineSensor
```

To run as a standalone application:
```
$ ./gradlew installDist
$ cd standalone-examples/build/install/pravega-standalone-examples
$ bin/turbineSensor [--stream <stream name>]

```