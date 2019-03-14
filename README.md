# Pravega and Analytics Connectors Examples

This repository contains code samples to demonstrate how developers can work with 
[Pravega](http://pravega.io). We also provide code samples to connect analytics 
engines such as [Flink](https://flink.apache.org/) and
[Hadoop](http://hadoop.apache.org/) with Pravega as a storage substrate for data 
streams. 

For more information on Pravega, we recommend to read the [documentation and the
developer guide](http://pravega.io).

# Repository Structure

This repository is divided into sub-projects (`pravega-client-examples`, `flink-connector-examples`
and `hadoop-connector-examples`), each one addressed to demonstrate a specific component. In these sub-projects, 
we provide a battery of simple code examples aimed at illustrating how a particular 
feature or API works. Moreover, we also include a `scenarios` folder that contains 
more complex applications as sub-projects, which show use-cases exploiting one or multiple components.

> Hint: Have a look to the [terminology and concepts](http://pravega.io/docs/latest/terminology/) in Pravega.

## Pravega Client Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `gettingstarted` | Simple example of how to read/write from/to a Pravega `Stream`. | [Java](pravega-client-examples/src/main/java/io/pravega/example/gettingstarted)
| `consolerw` | Application that allows users to work with `Stream`, `Transaction` and `StreamCut` APIs via CLI. | [Java](pravega-client-examples/src/main/java/io/pravega/example/consolerw)
| `noop` | Example of how to add a simple callback executed upon a read event. | [Java](pravega-client-examples/src/main/java/io/pravega/example/noop)
| `statesynchronizer` | Application that allows users to work with `StateSynchronizer` API via CLI. | [Java](pravega-client-examples/src/main/java/io/pravega/example/statesynchronizer)
| `streamcuts` | Application examples demonstrating the use of `StreamCut`s via CLI. | [Java](pravega-client-examples/src/main/java/io/pravega/example/streamcuts) 
| `streamprocessing` | An example that illustrates exactly-once processing using the Pravega API. | [Java](pravega-client-examples/src/main/java/io/pravega/example/streamprocessing)

The related documentation and instructions are [here](pravega-client-examples).

## Flink Connector Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `wordcount` | Counting the words continuously from a Pravega `Stream` to demonstrate the usage of Flink connector for Pravega. | [Java](flink-connector-examples/src/main/java/io/pravega/example/flink/wordcount)
| `primer` | This sample demonstrates Pravega "exactly-once" feature jointly with Flink checkpointing and exactly-once mode. | [Java](flink-connector-examples/src/main/java/io/pravega/example/flink/primer)
| `streamcuts` | This sample demonstrates the use of Pravega StreamCuts in Flink applications. | [Java](flink-connector-examples/src/main/java/io/pravega/example/flink/streamcuts)

The related documentation and instructions are [here](flink-connector-examples).

## Hadoop Connector Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `wordcount` | Counts the words from a Pravega `Stream` filled with random text to demonstrate the usage of Hadoop connector for Pravega. | [Java](hadoop-connector-examples/src/main/java/io/pravega/example/hadoop/wordcount)
| `terasort` | Sort events from an input Pravega `Stream` and then write sorted events to one or more streams. | [Java](hadoop-connector-examples/src/main/java/io/pravega/example/hadoop/terasort)

The related documentation and instructions are [here](hadoop-connector-examples).

## Scenarios
| Example Name   | Description  | Language |
| ------------- |:-----| :-----|
| [`turbineheatsensor`](scenarios/turbine-heat-sensor) | It emulates parallel sensors producing temperature values (writers) and parallel consumers performing real-time statistics (readers) via Pravega client. | [Java](scenarios/turbine-heat-sensor/src/main/java/io/pravega/turbineheatsensor)
| [`turbineheatprocessor`](scenarios/turbine-heat-processor) | A Flink streaming application for processing temperature data from a Pravega stream produced by the `turbineheatsensor` app. The application computes a daily summary of the temperature range observed on that day by each sensor. | [Java](scenarios/turbine-heat-processor/src/main/java/io/pravega/turbineheatprocessor), [Scala](scenarios/turbine-heat-processor/src/main/scala/io/pravega/turbineheatprocessor)
| [`anomaly-detection`](scenarios/anomaly-detection) | A Flink streaming application for detecting anomalous input patterns using a finite-state machine. | [Java](scenarios/anomaly-detection/src/main/java/io/pravega/anomalydetection)
| [`pravega-flink-connector-sql-samples`](scenarios/pravega-flink-connector-sql-samples) | Flink connector table api/sql samples. | [Java](scenarios/pravega-flink-connector-sql-samples/src/main/java/io/pravega/connectors.nytaxi)


# Build Instructions

Next, we provide instructions for building the `pravega-samples` repository. There are two main options: 
- _Out-of-the-box_: If you want a quick start, run the samples by building `pravega-samples` out-of-the-box
(go straight to section `Pravega Samples Build Instructions`). 
- _Build from source_: If you want to have fun building the different projects from source, please read
section `Building Pravega Components from Source (Optional)` before building `pravega-samples`. 

## Pre-requisites

* Java 8

## Building Pravega Components from Source (Optional)

### Pravega Build Instructions 

If you want to build Pravega from source, you may need to generate the latest Pravega `jar` files and install them to 
your local Maven repository. To build Pravega from sources and use it here, please run the following commands:

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
$ ./gradlew install
```

The above command should generate the required `jar` files into your local Maven repository.

> Hint: For using in the sample applications the Pravega version you just built, you need to update the 
`pravegaVersion=<local_maven_pravega_version>` property in `gradle.properties` file 
of `pravega-samples`.

For more information, please visit [Pravega](https://github.com/pravega/pravega).

### Flink Connector Build Instructions

To build the Flink connector from source, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone --recursive https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew install
```

> Hint: For using in the sample applications the Flink connector version you just built, you need to update the 
`flinkConnectorVersion=<local_maven_flink_connector_version>` property in `gradle.properties` file 
of `pravega-samples`.


For more information, please visit [Flink Connectors](https://github.com/pravega/flink-connectors). 

### Hadoop Connector Build Instructions

To build the Hadoop connector from source, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone --recurse-submodules https://github.com/pravega/hadoop-connectors.git
$ cd hadoop-connectors
$ ./gradlew install
```

> Hint: For using in the sample applications the Hadoop connector version you just built, you need to update the 
`hadoopConnectorVersion=<local_maven_hadoop_connector_version>` property in `gradle.properties` file 
of `pravega-samples`.


For more information, please visit [Hadoop Connectors](https://github.com/pravega/hadoop-connectors). 

### Configuring Pravega Samples for Running with Source Builds

In the previous instructions, we noted that you will need to change the `gradle.properties` file in
`pravega-samples` for using the Pravega components built from source. Here we provide an example of how to do so:

1) Imagine that we want to build Pravega from source. Let us assume that we 
executed `git clone https://github.com/pravega/pravega.git` and the last commit of 
`master` branch is `2990193xxx`. 

2) After executing `./gradlew install`, we will see in our local Maven repository 
(e.g., `~/.m2/repository/io/pravega/*`) artifacts that contain in their names that commit version 
such as `0.3.0-1889.2990193-SNAPSHOT`. These artifacts are the result from building Pravega from source. 

3) The only thing you have to do is to set `pravegaVersion=0.3.0-1889.2990193-SNAPSHOT` in the `gradle.properties`
file of `pravega-samples`.

While this example is for Pravega, the same procedure applies for Flink and Hadoop connectors.


## Pravega Samples Build Instructions

The `pravega-samples` project is prepared for working out-of-the-box with 
[release artifacts](https://github.com/pravega/pravega/releases) of Pravega components, which are already 
available in Maven central. To build `pravega-samples` from source, use the built-in gradle wrapper as follows:

```
$ git clone https://github.com/pravega/pravega-samples.git
$ cd pravega-samples
$ ./gradlew clean installDist
```
That's it! You are good to go and execute the examples :) 

To ease their execution, most examples can be run either using the gradle wrapper (gradlew) or scripts. 
The above gradle command automatically creates the execution scripts that can be found under:

```
pravega-samples/pravega-client-examples/build/install/pravega-client-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

_Working with `dev` branch_: If you are curious about the most recent sample applications, 
you may like to try the `dev` version of `pravega-samples` as well. To do so, just clone the 
`dev` branch instead of `master` (default): 

```
$ git clone -b dev https://github.com/pravega/pravega-samples.git
$ cd pravega-samples
$ ./gradlew clean installDist
```

The `dev` branch works with Pravega snapshots artifacts published in 
our [JFrog repository](https://oss.jfrog.org/artifactory/jfrog-dependencies/io/pravega/) instead of 
using release versions.


# Proposed Roadmap

We propose a roadmap to proceed with the execution of examples based on their complexity:
1. [Pravega client examples](pravega-client-examples): 
First step to understand the basics of Pravega and exercise the concepts presented in the documentation. 
2. [Flink connector examples](flink-connector-examples): 
These examples show the basic functionality of the Flink connector for Pravega.
3. [Hadoop connector examples](hadoop-connector-examples): 
These examples show the basic functionality of the Hadoop connector for Pravega.
4. [Scenarios](scenarios): Applications that go beyond the basic usage of Pravega APIs, which may include complex interactions 
between Pravega and analytics engines (e.g., Flink, Hadoop, Spark) to demonstrate analytics use cases.

# Where to Find Help

Documentation on Pravega and Analytics Connectors:
* [Pravega.io](http://pravega.io/), [Pravega Wiki](https://github.com/pravega/pravega/wiki).
* [Flink Connectors Wiki](https://github.com/pravega/flink-connectors/wiki).

Did you find a problem or bug?
* First, check our [FAQ](http://pravega.io/docs/latest/faq/).
* If the FAQ does not help you, create a [new GitHub issue](https://github.com/pravega/pravega-samples/issues).

Do you want to contribute a new example application?
* Follow the [guidelines for contributors](https://github.com/pravega/pravega/wiki/Contributing).

Have fun!!
