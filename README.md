# pravega-samples

Sample applications for Pravega.

## Getting Started
### Building Pravega

_Be sure to use the `r0.0-alpha` branch for compatibility with the Alpha release._

Install the Pravega client libraries to your local Maven repository:
```
./gradlew common:publishMavenPublicationToMavenLocal \
  controller:contract:publishMavenPublicationToMavenLocal \
  clients:streaming:publishMavenPublicationToMavenLocal \
  connectors:flink:publishShadowPublicationToMavenLocal
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
A Flink streaming application for processing temperature data from a Pravega stream.   Complements the Turbine Heat Sensor app (external).

Automatically creates a scope (`turbine`) and stream (`turbineHeatTest`) as necessary.

#### Running
Run the sample from the command-line:
```
$ bin/run-example.sh TurbineHeatProcessor [--controller <URI>] [--scope <name>] [--stream <name>] [--startTime <long>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-0.0-PRERELEASE-all.jar`
- Main class: `com.emc.pravega.examples.flink.TurbineHeatProcessor`
