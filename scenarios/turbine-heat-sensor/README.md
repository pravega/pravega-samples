# Turbine Heat Sensor
An example of a lightweight IOT application that writes simulated sensor events to a Pravega stream.

## Pre requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build `pravega-samples` repository
3. Apache Flink 1.12 running

# Execution

```
$ bin/turbineSensor [--stream <stream name>]
```
