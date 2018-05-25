# Pravega and Analytics Connectors Examples

This repository contains code samples to demonstrate how developers can work with 
[Pravega](http://pravega.io). We also provide code samples to connect analytics 
engines such as [Flink](https://flink.apache.org/) and
[Hadoop](http://hadoop.apache.org/) with Pravega as a storage substrate for data 
streams. 

For more information on Pravega, we recommend to read the [documentation and the
developer guide](http://pravega.io).

# Repository Structure

This repository is divided into sub-projects (`standalone-examples`, `flink-examples`
and `hadoop-examples`) addressed to demonstrate a specific component. In these sub-projects, 
we provide a battery of simple code examples aimed at demonstrating how a particular 
feature or API works. Moreover, we also include a `scenarios` sub-project that contains 
more complex applications, which show use-cases exploiting one or multiple components.

## Pravega Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `gettingstarted` | Simple example of how to read/write from/toa Pravega `Stream`. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/gettingstarted)
| `consolerw` | Application that allows users to work with `Stream`, `Transaction` and `StreamCut` APIs via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/consolerw)
| `noop` | Example of how to add a simple callback executed upon a read event. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/noop)
| `statesynchronizer` | Application that allows users to work with `StateSynchronizer` API via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/statesynchronizer)
| `streamcuts` | Application examples demonstrating the use of `StreamCut`s via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/streamcuts) 

## Flink-connector Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `wordcount` | Counting the words continuously from a Pravega `Stream` to demonstrate the usage of Flink connector. | [Java](https://github.com/pravega/pravega-samples/tree/master/flink-examples/src/main/java/io/pravega/examples/flink/wordcount)

## Scenarios
| Example Name   | Description  | Language |
| ------------- |:-----| :-----|
| `turbineheatsensor` | It emulates parallel sensors producing temperature values (writers) and parallel consumers performing real-time statistics (readers) via Pravega client. | [Java](https://github.com/pravega/pravega-samples/tree/master/standalone-examples/src/main/java/io/pravega/example/turbineheatsensor)
| `turbineheatprocessor` | A Flink streaming application for processing temperature data from a Pravega stream produced by the `turbineheatsensor` app. The application computes a daily summary of the temperature range observed on that day by each sensor. | [Java](https://github.com/pravega/pravega-samples/tree/master/flink-examples/src/main/java/io/pravega/examples/flink/iot), [Scala](https://github.com/pravega/pravega-samples/tree/master/flink-examples/src/main/scala/io/pravega/examples/flink/iot)
| `anomaly-detection` | A Flink streaming application for detecting anomalous input patterns using a finite-state machine. | [Java](https://github.com/pravega/pravega-samples/tree/master/anomaly-detection)


# Build Instructions

## Pre-requisites

* Java 8

* Maven 2

## Pravega Build Instructions (Optional)

For release builds of Pravega, the artifacts will already be in Maven Central and you will 
not need to run this step. Conversely, if you have downloaded a nightly build of Pravega, 
you may need to generate the latest Pravega `jar` files and publish them to your local Maven 
repository. 

To install the Pravega libraries to your local Maven repository:

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
$ ./gradlew install
```

The above command should generate the required `jar` files into your local Maven repository.
For more information, please visit [Pravega](https://github.com/pravega/pravega). 

> Hint: If you use a different version of Pravega, please check the gradle.properties file.

## Flink Connector Build Instructions

To execute Flink connector examples, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone --recursive https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew clean install
```

For more information, please visit [Flink Connectors](https://github.com/pravega/flink-connectors). 

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
under:

```
pravega-samples/standalone-examples/build/install/pravega-standalone-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

# Proposed Roadmap

We propose a roadmap to proceed with the execution of examples based on their complexity:
1. [Pravega examples](https://github.com/pravega/pravega-samples/tree/master/standalone-examples): 
First step to understand the basics of Pravega and exercise the concepts presented in the documentation. 
2. [Pravega scenarios](https://github.com/pravega/pravega-samples/tree/master/standalone-examples): 
Applications to demonstrate/show how applications can exploit Pravega once the developer is familiar with the main APIs.
3. [Flink-connector examples](https://github.com/pravega/pravega-samples/tree/master/flink-examples): 
These examples show the basic functionality of the Flink connector for Pravega.
4. [Flink-connector scenarios](https://github.com/pravega/pravega-samples/tree/master/flink-examples): 
Applications that go beyond the basic the interaction between Pravega and Flink to demonstrate complex analytics use cases.

# Where to find help

Documentation on Pravega and Analytics Connectors:
* [Pravega.io](http://pravega.io/), [Pravega Wiki](https://github.com/pravega/pravega/wiki).
* [Flink Connectors Wiki](https://github.com/pravega/flink-connectors/wiki).

Did you find a problem or bug?
* First, check our [FAQ](http://pravega.io/docs/latest/faq/).
* If the FAQ does not help you, create a [new GitHub issue](https://github.com/pravega/pravega-samples/issues).

Do you want to contribute a new example application?
* Follow the [guidelines for contributors](https://github.com/pravega/pravega/wiki/Contributing).

Have fun!!





