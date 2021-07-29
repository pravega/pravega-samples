# Flink Connector Examples for Pravega
Battery of code examples to demonstrate the capabilities of Pravega as a data stream storage 
system for Apache Flink. 

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
3. Apache Flink 1.12 running


## Distributing Flink Samples
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy 
uber-jar called `pravega-flink-examples-<VERSION>-all.jar`:

```bash
$ ./gradlew installDist
...
$ ls -R flink-connector-examples/build/install/pravega-flink-examples
flink-connector-examples/build/install/pravega-flink-examples:
bin  lib

flink-connector-examples/build/install/pravega-flink-examples/bin:
dataProducer      eventTimeAverage      exactlyOnceChecker      exactlyOnceWriter      pravegaWatermarkIngestion      sliceProcessor      streamBookmarker      wordCountReader      wordCountWriter
dataProducer.bat  eventTimeAverage.bat  exactlyOnceChecker.bat  exactlyOnceWriter.bat  pravegaWatermarkIngestion.bat  sliceProcessor.bat  streamBookmarker.bat  wordCountReader.bat  wordCountWriter.bat

flink-connector-examples/build/install/pravega-flink-examples/lib:
akka-actor_2.12-2.5.21.jar     flink-clients_2.12-1.12.4.jar                 flink-streaming-java_2.12-1.12.4.jar                                pravega-flink-examples-0.10.0-SNAPSHOT-all.jar
akka-protobuf_2.12-2.5.21.jar  flink-core-1.12.4.jar                         force-shading-1.12.4.jar                                            pravega-flink-examples-0.10.0-SNAPSHOT.jar
akka-slf4j_2.12-2.5.21.jar     flink-file-sink-common-1.12.4.jar             grizzled-slf4j_2.12-1.3.2.jar                                       pravega-keycloak-client-0.9.0.jar
akka-stream_2.12-2.5.21.jar    flink-hadoop-fs-1.12.4.jar                    javassist-3.24.0-GA.jar                                             reactive-streams-1.0.2.jar
chill-java-0.7.6.jar           flink-java-1.12.4.jar                         jsr305-1.3.9.jar                                                    reflectasm-1.11.7.jar
chill_2.12-0.7.6.jar           flink-metrics-core-1.12.4.jar                 kryo-2.24.0.jar                                                     scala-java8-compat_2.12-0.9.0.jar
commons-cli-1.3.1.jar          flink-optimizer_2.12-1.12.4.jar               kryo-5.0.0-RC1.jar                                                  scala-library-2.12.7.jar
commons-collections-3.2.2.jar  flink-queryable-state-client-java-1.12.4.jar  kryo-serializers-0.45.jar                                           scala-parser-combinators_2.12-1.1.1.jar
commons-compress-1.20.jar      flink-runtime_2.12-1.12.4.jar                 log4j-1.2.17.jar                                                    scopt_2.12-3.5.0.jar
commons-io-2.7.jar             flink-shaded-asm-7-7.1-12.0.jar               lz4-java-1.6.0.jar                                                  slf4j-api-1.7.25.jar
commons-lang3-3.3.2.jar        flink-shaded-guava-18.0-12.0.jar              minlog-1.2.jar                                                      slf4j-log4j12-1.7.25.jar
commons-math3-3.5.jar          flink-shaded-jackson-2.10.1-12.0.jar          minlog-1.3.0.jar                                                    snappy-java-1.1.8.3.jar
config-1.3.3.jar               flink-shaded-netty-4.1.49.Final-12.0.jar      objenesis-2.6.jar                                                   ssl-config-core_2.12-0.3.7.jar
flink-annotations-1.12.4.jar   flink-shaded-zookeeper-3-3.4.14-12.0.jar      pravega-connectors-flink-1.12_2.12-0.10.0-264.c00d735-SNAPSHOT.jar
```

Please note that the libs version might vary from time to time and the example program is an uber jar named `pravega-flink-examples-<samples version>-all.jar`

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