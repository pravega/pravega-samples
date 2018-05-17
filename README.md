# Pravega and Analytics Connectors Examples

This repository contains code examples to demonstrate how developers can work with 
[Pravega](http://pravega.io). We also provide code examples to connect analytics 
engines such as [Flink](https://flink.apache.org/) and
[Hadoop](http://hadoop.apache.org/) with Pravega as a storage substrate for data 
streams. 

For more information on Pravega, we recommend to read the [documentation and the
developer guide](http://pravega.io).

# Repository Structure

This repository is divided into sub-projects specifically addressed to demonstrate a
single component (`standalone-examples` and `flink-examples`). 
In each sub-project, we divided the examples into two categories: i) `features`, which are very simple
examples aimed at demonstrating how a particular feature or API works; and ii)
`scenarios`, which are more complex applications that show use-cases exploiting the 
component at hand.

## Pravega Examples
| Example Name        |  Type  | Description  | Language |
| ------------- |:-------------| :-----| :-----|
| `gettingstarted` | feature | Simple example of how to read/write a Pravega `Stream`. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/gettingstarted)
| `consolerw` | feature      | Application that allows users to work with `Stream`, `Transaction` and `StreamCut` APIs via CLI. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/consolerw)
| `noop` | feature      | Example of how to add a simple callback executed upon a read event. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/noop)
| `statesynchronizer` | feature | Application that allows users to work with `StateSynchronizer` API via CLI. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/statesynchronizer)
| `streamcuts` | feature | Application examples demonstrating the use of `StreamCut`s via CLI. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/streamcuts) 
| `turbineheatsensor` | scenario | It emulates parallel sensors producing temperature values (writers) and parallel consumers performing real-time statistics (readers). | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/turbineheatsensor)

## Flink-connector Examples
| Example Name        |  Type  | Description  | Language |
| ------------- |:-------------| :-----| :-----|
| `wordcount` | feature | Continuous count words on a Pravega `Stream` to demonstrate the usage of Flink connector. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/flink-examples/src/main/java/io/pravega/examples/flink/wordcount)
| `turbineheatprocessor` | scenario | A Flink streaming application for processing temperature data from a Pravega stream. Complements the `turbineheatsensor` app. The application computes a daily summary of the temperature range observed on that day by each sensor. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/flink-examples/src/main/java/io/pravega/examples/flink/iot), [Scala](https://github.com/RaulGracia/pravega-samples/tree/master/flink-examples/src/main/scala/io/pravega/examples/flink/iot)
| `anomaly-detection` | scenario | A Flink streaming application for detecting anomalous input patterns using a finite-state machine. | [Java](https://github.com/RaulGracia/pravega-samples/tree/master/anomaly-detection)

# Build Instructions

## Pre-requisites

* Java 8

* Maven 2

## Pravega Build Instructions (Optional)

If you have downloaded a nightly build of Pravega, you may need to generate the latest 
Pravega `jar` files and publish them to your local Maven repository. For release builds of 
Pravega, the artifacts will already be in Maven Central and you will not need to run this step.

To install the Pravega libraries to your local Maven repository:

```
$ git clone https://github.com/pravega/pravega.git
$ ./gradlew install
```

The above command should generate the required `jar` files into your local Maven repository.

> Hint: If you use a different version of Pravega, please check the gradle.properties file.

## Flink Connector Build Instructions

To execute Flink connector examples, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew clean install
```

## Pravega Samples Build Instructions

Finally, we need to build the code of the examples. To this end, use the built-in gradle wrapper 
to build the samples as follows:

```
$ git clone https://github.com/pravega/pravega-samples.git
$ cd pravega-samples
$ ./gradlew clean installDist
```
To ease their execution, most examples can be run either using the gradle wrapper (gradlew) or 
scripts. The above gradle command automatically creates the execution scripts that can be found
under the `pravega-samples` directory in:

```
standalone-examples/build/install/pravega-standalone-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

# Proposed Roadmap

We propose a roadmap to proceed with the execution of examples based on their complexity:
1. [Pravega features](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples): First step to understand the basics of 
Pravega and exercise the concepts presented in the documentation. 
2. [Pravega scenarios](https://github.com/RaulGracia/pravega-samples/tree/master/standalone-examples): These examples will demonstrate the 
developer how applications can exploit Pravega once s/he is familiar with the main APIs.
3. [Flink-connector features](https://github.com/RaulGracia/pravega-samples/tree/master/flink-examples): These examples will show 
the basic functionality of the Flink connector for Pravega.
4. [Flink-connector scenarios](https://github.com/RaulGracia/pravega-samples/tree/master/flink-examples): These examples go beyond
basic the interaction between Pravega and Flink to demonstrate complex analytics use cases.


Have fun!!





