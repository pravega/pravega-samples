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
and `hadoop-connector-examples`) addressed to demonstrate a specific component. In these sub-projects, 
we provide a battery of simple code examples aimed at demonstrating how a particular 
feature or API works. Moreover, we also include a `scenarios` sub-project that contains 
more complex applications, which show use-cases exploiting one or multiple components.

> Hint: Have a look to the [terminology and concepts](http://pravega.io/docs/latest/terminology/) in Pravega.

## Pravega Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `gettingstarted` | Simple example of how to read/write from/to a Pravega `Stream`. | [Java](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/gettingstarted)
| `consolerw` | Application that allows users to work with `Stream`, `Transaction` and `StreamCut` APIs via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/consolerw)
| `noop` | Example of how to add a simple callback executed upon a read event. | [Java](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/noop)
| `statesynchronizer` | Application that allows users to work with `StateSynchronizer` API via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/statesynchronizer)
| `streamcuts` | Application examples demonstrating the use of `StreamCut`s via CLI. | [Java](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/streamcuts) 

Please, find the related documentation and instructions [here](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples).

## Flink Connector Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `wordcount` | Counting the words continuously from a Pravega `Stream` to demonstrate the usage of Flink connector for Pravega. | [Java](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples/src/main/java/io/pravega/example/flink/wordcount)

Please, find the related documentation and instructions [here](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples).

## Hadoop Connector Examples
| Example Name  | Description  | Language |
| ------------- |:-----| :-----|
| `wordcount` | Counts the words from a Pravega `Stream` filled with random text to demonstrate the usage of Hadoop connector for Pravega. | [Java](https://github.com/pravega/pravega-samples/tree/master/hadoop-connector-examples/src/main/java/io/pravega/example/hadoop/wordcount)

Please, find the related documentation and instructions [here](https://github.com/pravega/pravega-samples/tree/master/hadoop-connector-examples).

## Scenarios
| Example Name   | Description  | Language |
| ------------- |:-----| :-----|
| [`turbineheatsensor`](https://github.com/pravega/pravega-samples/tree/master/scenarios/turbine-heat-sensor) | It emulates parallel sensors producing temperature values (writers) and parallel consumers performing real-time statistics (readers) via Pravega client. | [Java](https://github.com/pravega/pravega-samples/tree/master/scenarios/turbine-heat-sensor/src/main/java/io/pravega/turbineheatsensor)
| [`turbineheatprocessor`](https://github.com/pravega/pravega-samples/tree/master/scenarios/turbine-heat-processor) | A Flink streaming application for processing temperature data from a Pravega stream produced by the `turbineheatsensor` app. The application computes a daily summary of the temperature range observed on that day by each sensor. | [Java](https://github.com/pravega/pravega-samples/tree/master/scenarios/turbine-heat-processor/src/main/java/io/pravega/turbineheatprocessor), [Scala](https://github.com/pravega/pravega-samples/tree/master/scenarios/turbine-heat-processor/src/main/scala/io/pravega/turbineheatprocessor)
| [`anomaly-detection`](https://github.com/pravega/pravega-samples/tree/master/scenarios/anomaly-detection) | A Flink streaming application for detecting anomalous input patterns using a finite-state machine. | [Java](https://github.com/pravega/pravega-samples/tree/master/scenarios/anomaly-detection/src/main/java/io/pravega/anomalydetection)


# Build Instructions


The `pravega-samples` project is prepared for working with [release artifacts](https://github.com/pravega/pravega/releases) of Pravega components, 
which are already available in Maven central. Moreover, you also have the option to configure `pravega-samples`
to work with `master` snapshots artifacts published in our [JFrog repository](https://oss.jfrog.org/artifactory/jfrog-dependencies/io/pravega/).
You can easily do this by specifying the desired artifact version (`pravegaVersion`, `flinkConnectorVersion`,
`hadoopConnectorVersion`) in the `gradle.properties` file. Thus, if you look for a quick start 
you may skip the _optional_ sections; otherwise, if you want to have fun building the different projects from source,
please read the next sections. 


## Pre-requisites

* Java 8+

## Pravega Build Instructions (Optional)

If you want to build Pravega from source, you may need to generate the 
latest Pravega `jar` files and install them to your local Maven repository. 
To this end, please run the following commands:

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
$ ./gradlew install
```

The above command should generate the required `jar` files into your local Maven repository.

> Hint: If you use a different version of Pravega, please check the `pravegaVersion` property 
in `gradle.properties` file.

For more information, please visit [Pravega](https://github.com/pravega/pravega).

## Flink Connector Build Instructions (Optional)

To build the Flink connector from source, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone --recursive https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew clean install
```

> Hint: If you use a different version of Flink connector, please check the `flinkConnectorVersion` property 
in `gradle.properties` file.

For more information, please visit [Flink Connectors](https://github.com/pravega/flink-connectors). 

## Hadoop Connector Build Instructions (Optional)

To build the Hadoop connector from source, follow the below steps to build and publish artifacts from 
source to local Maven repository:

```
$ git clone --recurse-submodules https://github.com/pravega/hadoop-connectors.git
$ cd hadoop-connectors
$ gradle install
```

> Hint: If you use a different version of Hadoop connector, please check the `hadoopConnectorVersion` property 
in `gradle.properties` file.

For more information, please visit [Hadoop Connectors](https://github.com/pravega/hadoop-connectors). 

## Pravega Samples Build Instructions

Finally, we need to build the code of the examples. Note that the `master` branch points to release 
artifacts of Pravega and connectors, whereas the `develop` branch works with snapshot artifacts.
To build `pravega-samples` from source, use the built-in gradle wrapper as follows:

```
$ git clone https://github.com/pravega/pravega-samples.git
$ cd pravega-samples
$ ./gradlew clean installDist
```
To ease their execution, most examples can be run either using the gradle wrapper (gradlew) or 
scripts. The above gradle command automatically creates the execution scripts that can be found
under:

```
pravega-samples/pravega-client-examples/build/install/pravega-client-examples/bin
```

There is a Linux/Mac script and a Windows (.bat) script for each separate executable.

# Proposed Roadmap

We propose a roadmap to proceed with the execution of examples based on their complexity:
1. [Pravega examples](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples): 
First step to understand the basics of Pravega and exercise the concepts presented in the documentation. 
2. [Flink connector examples](https://github.com/pravega/pravega-samples/tree/master/flink-connector-examples): 
These examples show the basic functionality of the Flink connector for Pravega.
3. [Hadoop connector examples](https://github.com/pravega/pravega-samples/tree/master/hadoop-connector-examples): 
These examples show the basic functionality of the Hadoop connector for Pravega.
4. [Scenarios](https://github.com/pravega/pravega-samples/tree/master/scenarios): Applications that go beyond the basic usage of Pravega APIs, which may include complex interactions 
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





