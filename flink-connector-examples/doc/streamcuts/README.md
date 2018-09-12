# StreamCuts Flink Example

This sample application demonstrates the use of a Pravega abstraction, the `StreamCut`, in a Flink application to make 
it easier for developers to write analytics applications. Concretely, the objective of this sample is to demonstrate 
that: i) Flink applications may use `StreamCut`s to bookmark points of interest within a Pravega `Stream`, ii) `StreamCut`s 
can be stored as persistent bookmarks for a Pravega stream, iii) `StreamCut`s may be used to perform bounded stream/batch
processing on slices bookmarked by a Flink application. 

The example consists of three applications: `DataProducer`, `StreamBookmarker` and `SliceProcessor`.

```
$ cd flink-connector-examples/build/install/pravega-flink-examples
$ bin/dataProducer [--controller tcp://localhost:9090] [--num-events 10000]
$ bin/streamBookmarker [--controller tcp://localhost:9090] 
$ bin/sliceProcessor [--controller tcp://localhost:9090]
```

First, let us describe the role that these applications play in the sample:
- `DataProducer`: This application is intended to create data simulating various sensors publishing events. Each event
is a tuple `<sensorId, eventValue>`. In particular, the values of events follow a sinusoidal function to have intuitive
changing values within a controlled range for a demonstration.

- `StreamBookmarker`: This Flink application is intended to identify the slices of the `Stream` that we want to bookmark,
as they are of interest to execute further processing. That is, for a given `sensorId`, this job detects and stores the
start and end `StreamCut`s (i.e., `Stream` "slice") for which `eventValues` are `< 0`. With the start and end `StreamCut`s, 
this application creates a "slice" object that is persistently stored in a separate Pravega `Stream`.

- `SliceProcessor`: This application reads from the Pravega `Stream` where the start and end `StreamCut`s are published by
`StreamBookmarker`. For each stream slice object written in the `Stream`, this application executes (locally) a batch job 
that reads the events within the specified slice and performs some computation (e.g., counting events for the sensor
for which values are `< 0`). This is an example of bounded processing in Flink on a Pravega `Stream`.

## Execution

First of all, as this example consists of 3 applications, we recommend to open 3 different terminals. 
First, we run the `DataProducer` application:

```
$ bin/dataProducer --num-events 10000
```

Second, we run the `StreamBookmarker` application:

```
$ bin/streamBookmarker
```

And finally, the `SliceProcessor` application:

```
$ bin/sliceProcessor
```

Note that each application should run in a separate terminal.

## Interpreting the Output

The output in `DataProducer` is simple to interpret. It shows log messages in which every line corresponds to a new
`<sensorId, eventValue>` tuple. Note that we use as routing key for writing events the `sensorId`, so we keep strict
event ordering in a per-sensor basis:

```
18:42:20,561 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (0,0.983123065811597) (routing key 0).
18:42:20,562 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (1,0.6851269469128873) (routing key 1).
18:42:20,564 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (2,-0.2427717273527339) (routing key 2).
18:42:20,666 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (0,0.9849033340715402) (routing key 0).
18:42:20,669 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (1,0.6778085753923743) (routing key 1).
18:42:20,673 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (2,-0.25246026162814295) (routing key 2).
18:42:20,775 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (0,0.9865851128188263) (routing key 0).
18:42:20,776 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (1,0.6704224235791605) (routing key 1).
18:42:20,783 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (2,-0.26212355008777205) (routing key 2).
18:42:20,884 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (0,0.988168233876982) (routing key 0).
18:42:20,886 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (1,0.6629692300822725) (routing key 1).
18:42:20,887 WARN  io.pravega.example.flink.streamcuts.process.DataProducer      - Writing event: (2,-0.2717606264108279) (routing key 2).

```

Then, `StreamBookmarker` shows messages like the ones posted below:

```
18:41:07,291 WARN  io.pravega.example.flink.streamcuts.process.Bookmarker        - Start bookmarking a stream slice at: examples8/sensor-events:0=623580, 1=623295, 2=623580 for sensor 0.
...
18:41:43,136 WARN  io.pravega.example.flink.streamcuts.process.Bookmarker        - Initialize sensorStreamSlice end value to look for the next updated StreamCut: examples8/sensor-events:0=656070, 1=655785, 2=656070 for sensor 2.
...
18:41:48,277 WARN  io.pravega.example.flink.streamcuts.process.Bookmarker        - Found next end StreamCut for sensor 2: examples8/sensor-events:0=664050, 1=664050, 2=664050. The slice should contain all events < 0 for a specific sensor sine wave.
```

These messages correspond to the 3 actions that this application performs. First, in the context of a given `sensorId`
(i.e., keyed stream), this application detects the first time in which `eventValue < 0`. In that situation, the
task processing events for this `sensorId` calls to `readergroup.getStreamCuts()` to keep the `StreamCut` immediately 
preceding this event. This corresponds to the first message shown in the output.

A task continues processing events for a particular `sensorId` until if finds an `eventValue > 0`. As this application 
wants to bookmark slices of a `Stream` for which a sensor's events are `< 0`, it realizes that the slice should complete.
However, there is no guarantee that in the case of finding the first `eventValue > 0` we will have a `StreamCut`
representing this point in the Pravega `Stream` by calling `readergroup.getStreamCuts()` to form a slice containing all 
the events of interest. This is because the `StreamCut`s in a `ReaderGroup` are updated under certain situations, but 
not on every event read. In the case of a Flink application, one of the situations that trigger the update of a 
`ReaderGroup` `StreamCut`s are the execution of automatic checkpoints (which are associated with Pravega `Checkpoint`s). 
Therefore, the application stores the current `StreamCut` at this point to check for a proper `StreamCut` in the future. 
This corresponds to the second message shown in the output.

The application proceeds by checking whether the current `StreamCut` in the `ReaderGroup` is strictly higher than one
obtained for the first `eventValue > 0` the for all its common positions. When this condition is satisfied, we are sure
that the slice formed by the pair of `StreamCut`s will contain all the events of interest. In this case, we see a 
message as the third one in the output above. In this specific example, `StreamBookmarker` bookmarks a stream slice for 
which `sensorId=2` exhibited negative event values according to the following `StreamCuts`: 
from `<0=623580, 1=623295, 2=623580>` to `<0=664050, 1=664050, 2=664050>` (the Pravega `Stream` used had 3 segments, 
so a `StreamCut` provides a reading offset for each segment in the `Stream`).


Finally, `SliceProcessor` shows messages like the ones posted below:

```
18:41:48,314 WARN  io.pravega.example.flink.streamcuts.process.SliceProcessor    - Running batch job for slice: Start StreamCut: examples20/streamcuts-producer:0=0, 1=0, 2=0, end StreamCut: examples20/streamcuts-producer:0=129105, 1=123690, 2=129105, sensorId: 2.
...
18:41:53,395 WARN  io.pravega.example.flink.streamcuts.process.SliceProcessor    - Number of events < 0 in this slice for sensor 2: 314
```

`SliceProcessor` keeps reading from the `Stream` where slice objects are store by `StreamBookmarker`. When a new
slice is read, `SliceProcessor` shows a log message as the first shown in the output. In this application we use bounded
processing events which are defined by the input `StreamCut`s. Concretely, the processing tasks consists of counting the
number of negative events in the slice for a specific sensor. The result of this computation corresponds to the second
message of the output.