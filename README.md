# pravega-samples

Sample applications for Pravega.

## Getting Started

### Building Pravega

Optional: This step is required only if you want to use a different version
of Pravega than is published to maven central.

Install the Pravega client libraries to your local Maven repository:
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Building the Flink Connector

Optional: This step is required only if you want to use a different version
of Pravega than is published to maven central.

Install the shaded Flink Connector library to your local Maven repository:
```
$ git clone https://github.com/pravega/flink-connectors.git
$./gradlew install
```

### Building the Samples
Use the built-in gradle wrapper to build the samples.
```
$ ./gradlew build
...
BUILD SUCCESSFUL
```

### Distributing (Flink Samples)
#### Assemble
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy uber-jar called `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`.
```
$ ./gradlew installDist
...
$ ls -R flink-examples/build/install/pravega-flink-examples
bin	lib

flink-examples/build/install/pravega-flink-examples/bin:
run-example.sh

flink-examples/build/install/pravega-flink-examples/lib:
pravega-flink-examples-0.1.0-SNAPSHOT-all.jar
```

#### Upload
The `upload` task makes it easy to upload the sample binaries to your cluster.  First, configure Gradle
with the address of a node in your cluster.   Edit `~/.gradle/gradle.properties` to specify a value for `dcosAddress`.

```
$ cat ~/.gradle/gradle.properties
dcosAddress=10.240.124.164
```

Then, upload the samples to the cluster.  They'll be copied to `/home/centos` on the target node.
```
$ ./gradlew upload
```

## Flink Samples

### Turbine Heat Processor
A Flink streaming application for processing temperature data from a Pravega stream.   Complements the Turbine Heat Sensor app (external).   The application computes a daily summary of the temperature range observed on that day by each sensor.

Automatically creates a scope (`turbine`) and stream (`turbineHeatTest`) as necessary.

#### Running
Run the sample from the command-line:
```
$ bin/run-example.sh [--controller <URI>] [--scope <name>] [--stream <name>]
                     [--startTime <long>] [--output <path>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`
- Main class: `io.pravega.examples.flink.iot.TurbineHeatProcessor`

#### Outputs
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

