# Pravega Flink Connector Samples
Steps to set up and run Pravega Flink connector samples.

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

## Word Count Sample

This example demonstrates how to use the Pravega Flink Connectors to write data collected
from an external network stream into a Pravega stream and read the data from the Pravega stream.
See [Flink Word Count Sample](doc/flink-wordcount/README.md) for instructions.

## High Error Count Alert

This example demonstrates how to use the Pravega Flink connectors to read and 
parse Apache access logs from logstash via the [logstash pravega output plugin](https://github.com/pravega/logstash-output-pravega),
and how to generate alert when error count is high within a time frame. 
See [High Error Count Alert](doc/flink-high-error-count-alert/README.md) for instructions.

## Exactly Once Sample

This sample demonstrates Pravega EXACTLY_ONCE feature in conjuction with Flink checkpointing and exactly-once mode.
See [Exactly Once Sample](doc/exactly-once/README.md) for instructions.

