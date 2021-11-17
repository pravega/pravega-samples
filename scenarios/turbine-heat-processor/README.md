# Turbine Heat Processor
A Flink streaming application for processing temperature data from a Pravega `Stream`. 
Complements the Turbine Heat Sensor app 
([`turbineheatsensor`](../turbine-heat-sensor)). 
The application computes a daily summary of the temperature range observed on that day by each sensor.

Automatically creates a scope (default: `examples`) and stream (default: `turbineHeatTest`) as necessary.

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
3. Apache Flink 1.12 running


## Execution
Run the sample from the command-line:
```
$ bin/run-example [--controller <URI>] [--input <scope>/<stream>] [--startTime <long>] [--output <path>]
```

Alternately, run the sample from the Flink UI.
- JAR: `pravega-flink-examples-<VERSION>-all.jar`
- Main class: `io.pravega.turbineheatprocessor.TurbineHeatProcessor` or `io.pravega.turbineheatprocessor.TurbineHeatProcessorScala`

## Outputs
The application outputs the daily summary as a comma-separated values (CSV) file, one line per sensor per day. The data is
also visable in the Flink UI. For example:

```
...
SensorAggregate{startTime=28800000, sensorId=2, location='Arizona', tempMin=50.0, tempMax=90.0}
SensorAggregate{startTime=28800000, sensorId=18, location='Maine', tempMin=30.0, tempMax=70.0}
SensorAggregate{startTime=28800000, sensorId=10, location='Hawaii', tempMin=40.0, tempMax=80.0}
SensorAggregate{startTime=28800000, sensorId=3, location='Arkansas', tempMin=60.0, tempMax=100.0}
SensorAggregate{startTime=28800000, sensorId=0, location='Alabama', tempMin=50.0, tempMax=90.0}
SensorAggregate{startTime=28800000, sensorId=17, location='Louisiana', tempMin=70.0, tempMax=110.0}
SensorAggregate{startTime=28800000, sensorId=19, location='Maryland', tempMin=60.0, tempMax=100.0}
...
```
