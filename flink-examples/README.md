# Pravega Flink Connector Examples
Steps to set up and run Pravega Flink connector samples.

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build `flink-connector` repository
3. Build `pravega-samples` repository
4. Apache Flink running


### Distributing Flink Samples
#### Assemble
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy 
uber-jar called `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`:

```
$ ./gradlew installDist
...
$ ls -R flink-examples/build/install/pravega-flink-examples
bin	lib

flink-examples/build/install/pravega-flink-examples/bin:
run-example

flink-examples/build/install/pravega-flink-examples/lib:
pravega-flink-examples-0.1.0-SNAPSHOT-all.jar
```

#### Upload
The `upload` task makes it easy to upload the sample binaries to your cluster. First, configure 
Gradle with the address of a node in your cluster.   Edit `~/.gradle/gradle.properties` to 
specify a value for `dcosAddress`.

```
$ cat ~/.gradle/gradle.properties
dcosAddress=10.240.124.164
```

Then, upload the samples to the cluster.  They'll be copied to `/home/centos` on the target node.
```
$ ./gradlew upload
```

---

# Feature Examples Catalog

## Word Count Sample

This example demonstrates how to use the Pravega Flink Connectors to write data collected
from an external network stream into a Pravega stream and read the data from the Pravega stream.
See [Flink Word Count Sample](doc/flink-wordcount/README.md) for execution instructions.

---

# Scenario Examples Catalog

## Turbine Heat Processor
A Flink streaming application for processing temperature data from a Pravega stream. 
Complements the Turbine Heat Sensor app (external). The application computes a daily summary of 
the temperature range observed on that day by each sensor.

Automatically creates a scope (default: `examples`) and stream (default: `turbineHeatTest`) as 
necessary.

### Execution
Run the sample from the command-line:
```
$ bin/run-example [--controller <URI>] [--input <scope>/<stream>] [--startTime <long>] [--output <path>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`
- Main class: `io.pravega.examples.flink.iot.TurbineHeatProcessor` or `io.pravega.examples.flink.iot.TurbineHeatProcessorScala`

### Outputs
The application outputs the daily summary as a comma-separated values (CSV) file, one line per sensor per day.   The data is
also emitted to stdout (which may be viewed in the Flink UI).  For example:

```
...
SensorAggregate(1065600000,12,Illinois,(60.0,100.0))
SensorAggregate(1065600000,3,Arkansas,(60.0,100.0))
SensorAggregate(1065600000,7,Delaware,(60.0,100.0))
SensorAggregate(1065600000,15,Kansas,(40.0,80.0))
SensorAggregate(1152000000,3,Arkansas,(60.0,100.0))
SensorAggregate(1152000000,12,Illinois,(60.0,100.0))
SensorAggregate(1152000000,15,Kansas,(40.0,80.0))
SensorAggregate(1152000000,7,Delaware,(60.0,100.0))
...
```

## Anomaly Detection
A Flink streaming application for detecting anomalous input patterns using a finite-state machine.

_See the [anomaly-detection](https://github.com/pravega/pravega-samples/tree/master/anomaly-detection) directory for more information._

