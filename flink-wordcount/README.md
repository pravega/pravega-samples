# Word Count Example Using Pravega Flink Connectors
This example demonstrates how to use the Pravega Flink Connectors to write data collected from an external network stream into a Pravega stream and read the data from the Pravega stream.

## Pre requisites
1. Java 8
2. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)

## Build Pravega Flink Connectors

Follow the below steps to build and publish artifacts from source to local Maven repository:

```
$ git clone https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew clean install
```

## Build the Sample Code

Follow the below steps to build the sample code:

```
$ git clone https://github.com/pravega/pravega-samples.git
$ cd pravega-samples
$ ./gradlew clean installDist
```

## Word Count Example
This example consists of two applications, a WordCountWriter that reads data from a network stream, transforms the data, and writes the data to a Pravega stream; and a WordCountReader that reads from a Pravega stream and prints the word counts summary. You might want to run WordCountWriter in one window and WordCountReader in another.

The scripts can be found under the flink-wordcount directory in:
```
flink-wordcount/build/install/pravega-flink-wordcount/bin
```

### WordCountWriter
The application reads text from a socket, once every 5 seconds prints the distinct words and counts from the previous 5 seconds, and writes the word counts to a Pravega stream and prints word counts.

First, use netcat to start local server via
```
$ nc -lk 9999
```

Then start the WordCountWriter
```
$ bin/wordCountWriter [-host localhost] [-port 9999] [-stream myscope/wordcount] [-collector tcp://localhost:9090]
```

All args are optional, if not included, the defaults are:

 * host - "localhost"
 * port - "9999"
 * stream - "myscope/wordcount"
 * controller - "tcp://localhost:9090"

Now in the windows where netcat is running, enter some text, for example,
```
$ nc -lk 9999
aa bb cc aa
```
In the windows where wordCountWriter is running, it should show:
```
3> Word: aa:  Count: 2
4> Word: cc:  Count: 1
3> Word: bb:  Count: 1
```

### WordCountReader
The application reads data from a Pravega stream and prints the data.
```
$ bin/wordCountReader [-stream myscope/wordcount] [-collector tcp://localhost:9090]
```
All args are optional, if not included, the defaults are:
 * stream - "myscop/wordcount"
 * controller - "tcp://localhost:9090"

Example output:
```
4> Word: cc:  Count: 1
4> Word: aa:  Count: 2
4> Word: bb:  Count: 1
```
 
