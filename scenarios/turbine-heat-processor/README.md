# Turbine Heat Processor
A Flink streaming application for processing temperature data from a Pravega `Stream`. 
Complements the Turbine Heat Sensor app 
([`turbineheatsensor`](https://github.com/pravega/pravega-samples/scenarios/turbine-heat-sensor)). 
The application computes a daily summary of the temperature range observed on that day by each sensor.

Automatically creates a scope (default: `examples`) and stream (default: `turbineHeatTest`) as necessary.

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [flink-connectors](https://github.com/pravega/flink-connectors) repository
3. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
4. Apache Flink running


## Execution
Run the sample from the command-line:
```
$ bin/run-example [--controller <URI>] [--input <scope>/<stream>] [--startTime <long>] [--output <path>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`
- Main class: `io.pravega.turbineheatprocessor.TurbineHeatProcessor` or `io.pravega.turbineheatprocessor.TurbineHeatProcessorScala`

## Outputs
The application outputs the daily summary as a comma-separated values (CSV) file, one line per sensor per day. The data is
also emitted to stdout (which may be viewed in the Flink UI). For example:

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

