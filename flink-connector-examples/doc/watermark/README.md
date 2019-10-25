# Pravega Watermark Flink Example 

Flink offers [event-time characteristic](https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_time.html).
The mechanism in Flink to measure progress in event time is watermarks.
Watermarks flow as part of the data stream and carry a timestamp `t`.
A `Watermark(t)` declares that event time has reached time `t` in that stream, meaning that there should be no more elements from the stream with a timestamp `t’` <= `t` 
(i.e. events with timestamps older or equal to the watermark).

Pravega has innovated a strategy to generate the watermark within the stream in 0.6 release.
The main design is close to the concept in Flink.
The new Pravega watermark API enables the writer to provide event time information, and provide a time bound from the writers to the readers so that they can identify where they are in the stream.

This approach is integrated with Flink API.

The following two examples will show how to utilize Pravega watermark in Flink applications.

## Readers

There is a slight difference that Flink forces that event timestamp can be extracted from each record, while Pravega as a streaming storage doesn't have that limitation.

In order to enable Pravega watermark to transfer into Flink, Flink readers accepts an implementation of
1. How to extract timestamp from each record
2. How to leverage the watermark timestamp given the time bound from Pravega readers.

This example consists of three applications.
1. `PravegaIngestion` is to generate synthetic sensor data with event time and ingest into a Pravega stream.
2. `FlinkWatermarkReader` reads data from the stream with Pravega watermark, calculates an average value for each sensor under a 10-second window under event-time clock and prints the summary.

The scripts can be found under the flink-examples directory in:
```
flink-connector-examples/build/install/pravega-flink-examples/bin
```

Start the FlinkWatermarkReader app in one window by
```
$ bin/flinkWatermarkReader [-controller tcp://localhost:9090]
```

and then start PravegaWatermarkIngestion app in another
```
$ bin/pravegaWatermarkIngestion [-controller tcp://localhost:9090]
```

Every around 10 seconds, the average result for each sensor will be printed out.

## Writers

The application reads text from a socket, assigns the event time and then propagating the Flink watermark into Pravega with `enableWatermark(true)` in the Flink writer.

First, use `netcat` to start local server via
```
$ nc -lk 9999
```

Then start the `FlinkWatermarkWriter`:
```
$ bin/flinkWatermarkWriter [-controller tcp://localhost:9090]
```

As data comes into the socket, the pipeline will write these data into Pravega stream with watermark.

## Further readings
Flink implements many techniques from the Dataflow Model, and Pravega aligns with it.
For a better knowledge about event time and watermarks, the following articles can be really helpful.

- [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by Tyler Akidau
- The [Dataflow Model paper](https://research.google.com/pubs/archive/43864.pdf)
- Flink introductions for [event time](https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_time.html).
- Pravega design detail [PDP-33](https://github.com/pravega/pravega/wiki/PDP-33:-Watermarking)