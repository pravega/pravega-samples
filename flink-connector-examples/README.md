# Flink Connector Examples for Pravega
Battery of code examples to demonstrate the capabilities of Pravega as a data stream storage 
system for Apache Flink. 

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
3. Apache Flink running


## Distributing Flink Samples
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy 
uber-jar called `pravega-flink-examples-<VERSION>-all.jar`:

```
$ ./gradlew installDist
...
$ ls -R flink-connector-examples/build/install/pravega-flink-examples
bin	lib

flink-connector-examples/build/install/pravega-flink-examples/bin:
run-example

flink-connector-examples/build/install/pravega-flink-examples/lib:
pravega-flink-examples-VERSION-all.jar
```

---

# Examples Catalog

## Word Count

This example demonstrates how to use the Pravega Flink Connectors to write data collected
from an external network stream into a Pravega `Stream` and read the data from the Pravega `Stream`.
See [wordcount](doc/flink-wordcount/README.md) for more information and execution instructions.


## Exactly Once Sample

This sample demonstrates Pravega EXACTLY_ONCE feature in conjuction with Flink checkpointing and exactly-once mode.
See [Exactly Once Sample](doc/exactly-once/README.md) for instructions.


## StreamCuts Sample

This sample demonstrates the use of Pravega StreamCuts in Flink applications.
See [StreamCuts Sample](doc/streamcuts/README.md) for instructions.

## Pravega Watermark Sample

This sample demonstrates the use of Pravega Watermarks in Flink applications.
See [Watermark Sample](doc/watermark/README.md) for instructions.