# Simple sample Pravega reader and writer.

## Pre requisites
1. download pravega from https://github.com/pravega/pravega
2. start the singleNode version by running

```
./gradlew startSingleNode
```

## Publish Pravega jars to local Maven (optional)
If you have downloaded a nightly build of Pravega, you may need to generate the latest Pravega jar files and publish them to your local Maven repository.

Note: maven 2 needs to be installed and running on your machine

In the root of Pravega (where Pravega's build.gradle file can be found), run:

```
./gradlew common:publishMavenPublicationToMavenLocal controller:contract:publishMavenPublicationToMavenLocal clients:streaming:publishMavenPublicationToMavenLocal
```

The above command should generate the required jar files into your local maven repo.

## HelloWorldReader
A simple application that shows how to read from a Pravega stream.

Run by:

```
./gradlew startHelloWorldReader
```

or if you want don't want to take the defaults, use:

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

Run by:

```
./gradlew startHelloWorldWriter
```

or if you want don't want to take the defaults, use:

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