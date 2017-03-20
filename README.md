# pravega-samples

Sample applications for Pravega.

## Getting Started
### Building
Use the built-in gradle wrapper to build the samples.
```
$ ./gradlew build
...
BUILD SUCCESSFUL
```

### Distributing (Flink Samples)
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy uber-jar called `pravega-flink-examples-0.0-PRERELEASE-all.jar`.   
```
$ ./gradlew installDist
...
$ ls -R pravega-flink-examples/build/install/pravega-flink-examples
bin	lib

pravega-flink-examples/build/install/pravega-flink-examples/bin:
run-example.sh

pravega-flink-examples/build/install/pravega-flink-examples/lib:
pravega-flink-examples-0.0-PRERELEASE-all.jar
```

## Flink Samples

### Turbine Heat Processor
A Flink streaming application for processing temperature data from a Pravega stream.   Complements the Turbine Heat Sensor app (external).

Automatically creates a scope ('turbine') and stream (`turbineHeatTest`) as necessary.

#### Running
Run the sample from the command-line:
```
$ bin/run-example.sh TurbineHeatProcessor [--controller <URI>] [--scope <name>] [--stream <name>] [--startTime <long>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-0.0-PRERELEASE-all.jar`
- Main class: `com.emc.pravega.examples.flink.TurbineHeatProcessor`
