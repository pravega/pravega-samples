# Pravega Watermark Flink Example 

## Event time and Watermark Introduction
Flink offers [event-time characteristic](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/).
The mechanism in Flink to measure progress in event time is watermarks.
Watermarks flow as part of the data stream and carry a timestamp `t`.
A `Watermark(t)` declares that event time has reached time `t` in that stream, meaning that there should be no more elements from the stream with a timestamp `t'` <= `t` 
(i.e. events with timestamps older or equal to the watermark).

Pravega has innovated a strategy to generate the watermark within the stream in 0.6 release.
The main design is close to the concept in Flink.
The new Pravega watermark API enables the writer to provide event time information, and provide a time bound from the writers to the readers so that they can identify where they are in the stream.

This approach is integrated with Flink API.

The following examples will show how to utilize Pravega watermark in Flink applications.

## Application: Raw Data Ingestion

`PravegaWatermarkIngestion` is a Pravega writer to generate synthetic sensor data with event time and ingest into a Pravega stream.
It mocks a sine wave for three sensors and emits data every second in event time.

## Application: Event Time Window Average

### Usage of Pravega source with watermark 
There is a slight difference that Flink forces that event timestamp can be extracted from each record, while Pravega as a streaming storage doesn't have that limitation.

In order to enable Pravega watermark to transfer into Flink, Flink readers accepts an implementation of
1. How to extract timestamp from each record
2. How to leverage the watermark timestamp given the time bound from Pravega readers.

This thought is abstracted into an interface called `AssignerWithTimeWindows`. It's up to you to implement it.
While building the reader, please use `withTimestampAssigner(new MyAssignerWithTimeWindows())` to register the assigner. 

### Usage of Pravega sink with watermark 
The application reads text from a socket, assigns the event time and then propagating the Flink watermark into Pravega with `enableWatermark(true)` in the Flink writer.

### Our Recursive Application
`EventTimeAverage` reads sensor data from the stream with Pravega watermark, calculates an average value for each sensor under an fixed-length event-time window and generates the summary sensor data back into another Pravega stream.
You can run it recursively by reusing the result of the smaller window.

## How to run the application and verify
The scripts can be found under the flink-examples directory in:
```
$ cd flink-connector-examples/build/install/pravega-flink-examples/bin
```

Start the `PravegaWatermarkIngestion` app in one window by
```
$ bin/pravegaWatermarkIngestion [-controller tcp://localhost:9090]
```

and then start several `EventTimeAverage` applications in parallel

Required parameters:
1. `input` for input stream Name, default is `raw-data` which contains data from `PravegaWatermarkIngestion`
2. `output` for output stream Name
3. `window` for event time window length, set it increasingly in the recursive runs.
For example,
```
$ bin/eventTimeAverage [-controller tcp://localhost:9090] --input raw-data --output avg-10s --window 10
$ bin/eventTimeAverage [-controller tcp://localhost:9090] --input avg-10s --output avg-500s --window 500
$ bin/eventTimeAverage [-controller tcp://localhost:9090] --input avg-500s --output avg-final --window 10000
```

You can see the windowing average statistics recursively output for each job with different window lengths.
All the jobs are running under event time clock perfectly.

Sample output:

```
// 10s avg
...
2> SensorData{sensorId=2, value=-0.05747643363915676, timestamp=2017-07-14T11:19:00.000+0800}
2> SensorData{sensorId=0, value=-0.8801719658380192, timestamp=2017-07-14T11:19:00.000+0800}
2> SensorData{sensorId=1, value=-0.8677071977053602, timestamp=2017-07-14T11:19:00.000+0800}
2> SensorData{sensorId=2, value=0.25258416246753035, timestamp=2017-07-14T11:19:10.000+0800}
2> SensorData{sensorId=1, value=-0.674177582371591, timestamp=2017-07-14T11:19:10.000+0800}
2> SensorData{sensorId=0, value=-0.9811035671074867, timestamp=2017-07-14T11:19:10.000+0800}
...
```

```
// 500s avg from 10s
...
2> SensorData{sensorId=2, value=-0.012627637468484046, timestamp=2017-07-14T11:05:00.000+0800}
2> SensorData{sensorId=1, value=0.09979287923602662, timestamp=2017-07-14T11:05:00.000+0800}
2> SensorData{sensorId=0, value=0.12046428298937613, timestamp=2017-07-14T11:05:00.000+0800}
2> SensorData{sensorId=1, value=-0.09979287923602662, timestamp=2017-07-14T11:13:20.000+0800}
2> SensorData{sensorId=0, value=-0.12046428298937613, timestamp=2017-07-14T11:13:20.000+0800}
2> SensorData{sensorId=2, value=0.012627637468484188, timestamp=2017-07-14T11:13:20.000+0800}
...
```

## Further readings
Flink implements many techniques from the Dataflow Model, and Pravega aligns with it.
For a better knowledge about event time and watermarks, the following articles can be helpful.

- [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by Tyler Akidau
- The [Dataflow Model paper](https://research.google.com/pubs/archive/43864.pdf)
- Flink introductions for [event time](https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_time.html).
- Pravega design detail [PDP-33](https://github.com/pravega/pravega/wiki/PDP-33-%28Watermarking%29)
