# Pravega Stream Processing Example

# Overview

The examples in this directory are intended to illustrate how at-least-once semantics can be achieved with Pravega.
These show how to use Pravega directly, without relying on an external framework such as Apache Flink.

These examples include:

- [EventGenerator](EventGenerator.java):
  This application generates new events every 1 second
  and writes them to a Pravega stream (referred to as stream1).

- [AtLeastOnceApp](AtLeastOnceApp.java):
  This application demonstrates reading events from a Pravega stream (stream1), processing each event,
  and writing each output event to another Pravega stream (stream2).
  It guarantees that each event is processed at least once, even in the presence of failures.
  If multiple instances of this application are executed using the same Reader Group name,
  each instance will get a distinct subset of events, providing load balancing and redundancy.
  The user-defined processing function must be stateless.
  
  There is no direct dependency on other systems, except for Pravega.
  In particular, this application does not *directly* use a distributed file system, nor ZooKeeper.
  Coordination between processes occurs only through Pravega streams such as
  Reader Groups and State Synchronizers.

- [EventDebugSink](EventDebugSink.java):
  This application reads events from stream2 and displays them on the console.

# How to Run

- (Optional) Enable INFO (or DEBUG) level logging by editing the file [logback.xml](../../../../../resources/logback.xml).
  Update it to include:
  ```
  <root level="INFO">
  ```

-  Start the event generator.
   ```shell script
   cd ~/pravega-samples
   ./gradlew pravega-client-examples:startEventGenerator
   ```

   All parameters are provided as environment variables.
   You can either set them in your shell (`export PRAVEGA_SCOPE=examples`) or use the below syntax.

   If you are using a non-local Pravega instance, specify the controller as follows:
   ```shell script
   PRAVEGA_CONTROLLER=tcp://pravega.example.com:9090 ./gradlew pravega-client-examples:startEventGenerator
   ```

   Multiple parameters can be specified as follows.
   ```shell script
   PRAVEGA_SCOPE=examples PRAVEGA_CONTROLLER=tcp://localhost:9090 ./gradlew pravega-client-examples:startEventGenerator
   ```

   See [AppConfiguration.java](AppConfiguration.java) for available parameters.

- In another window, start one or more instances of the stream processor.
  The `runAtLeastOnceApp.sh` script can be used to run multiple instances concurrently.
  
  ```shell script
  cd ~/pravega-samples
  scripts/runAtLeastOnceApp.sh 2
  ```
  
  You may view the log files `tmp/atLeastOnceApp-*.log`.

- Start the event debug sink:
  ```shell script
  ./gradlew pravega-client-examples:startEventDebugSink
  ```

# Parallelism

A Reader Group will provide load balancing by assigning each reader a distinct set of segments (partitions).
Readers may not share segments. 
If there are more readers than segments, the extra readers will be idle.
The number of segments is controlled by the stream's scaling policy.
A scaling policy can specify a fixed number of segments or a dynamic number of segments based on
the target write rate, measured as events/sec or bytes/sec.

# Achieving At-Least-Once Semantics

## Reader Groups

Pravega uses a [Reader Group](http://pravega.io/docs/latest/reader-group-design/) to coordinate 
load balancing across multiple readers of a Pravega stream.
A Reader Group tracks the position (byte offsets) of each reader.
The Reader Group is the central building block for the at-least-once processor.

## Checkpoints

A checkpoint can be initiated on a Reader Group by writing a `ReaderGroupState.CreateCheckpoint` event, 
causing all readers to receive a checkpoint event when they call `EventStreamReader.readNextEvent()`.
When a reader receives a checkpoint event,
it is expected to flush any pending writes and then it will notify the Reader Group by writing a 
`ReaderGroupState.CheckpointReader` event as represented below.

```java
class CheckpointReader extends ReaderGroupStateUpdate {
    String checkpointId;
    String readerId;
    Map<Segment, Long> positions;  // map from segment to byte offset of the next event to read
    // ...
}
```

By default, checkpoints are initiated automatically by the Reader Group.
The interval can be specified using `ReaderGroupConfig.automaticCheckpointIntervalMillis()`.

## Graceful shutdown

A graceful shutdown of an at-least-once process requires that it call
`ReaderGroup.readerOffline(readerId, lastPosition)`, where `lastPosition`
is the position object of the last event that was successfully processed.
This method is called automatically when the `EventStreamReader` is closed.
When graceful shutdowns occur, events will be processed exactly once.

## Ungraceful shutdown

An ungraceful shutdown of a process can occur when the host abruptly loses power,
when it loses its network connection, or when the process is terminated with `kill -9`.
In these situations, the process is unable to successfully call `ReaderGroup.readerOffline()`.
However, the Reader Group guarantees that any assigned segments will remain assigned
to the reader until `readerOffline()` is called. 
If no action is taken, the events in the segments assigned to the dead worker's reader will never be read.
Therefore, we must have another process that is able to detect any dead workers
and call `readerOffline(readerId, lastPosition)` on their behalf.
The `ReaderGroupPruner` class provides this functionality.
Each worker process runs an instance of `ReaderGroupPruner`. 
When it detects a dead worker, it calls `readerOffline(readerId, lastPosition)` with
a null value for `lastPosition`. 
The null value for `lastPosition` indicates that it should use the position stored in the
last `ReaderGroupState.CheckpointReader` event in the Reader Group.
Any events processed by the now-dead worker after the last checkpoint will be reprocessed by other workers.
If writes are not idempotent, this will produce duplicates.

In order to detect dead workers, each worker process must run an instance of `ReaderGroupPruner`.
`ReaderGroupPruner` uses a [MembershipSynchronizer](MembershipSynchronizer.java) which uses a 
[State Synchronizer](http://pravega.io/docs/latest/state-synchronizer-design/) to
maintain the set of workers that are providing heart beats.
Each worker sends a heart beat by adding an update to the `MembershipSynchronizer`'s State Synchronizer.
Workers that fail to provide a heart beat after 10 intervals will be removed from the `MembershipSynchronizer`.
Finally, any workers in the Reader Group that are not in the `MembershipSynchronizer` are
considered dead and `readerOffline(readerId, null)` will be called by one or more instance of `ReaderGroupPruner`.

## Stream Life Cycle

The first instance of `AtLeastOnceApp` will create new State Synchronizer streams for the Reader Group and the `MembershipSynchronizer`.
All subsequent instances will use the existing streams. 
It is expected that the number of worker processes may scale up and down and may even scale to zero processes during maintenance windows.
To ensure that the position of readers is not lost, the `AtLastOnceApp` will not automatically delete these streams.
These streams can be deleted manually when it is known that the application will not need to restart from the
stored positions.

# Achieving Exactly Once Semantics

Exactly-once semantics can be achieved by using an idempotent writer with an at-least-once processor.
The `AtLeastOnceApp` writes its output to a Pravega stream using `EventStreamWriter` which is *not* idempotent.
However, if this were modified to write to a key/value store or relational database
with a deterministic key, then the writer would be idempotent and the system would provide exactly-once semantics.

# Stateful Exactly-once Semantics with Apache Flink

Although achieving at-least-once semantics with a parallel stateless processor is relatively simple
as shown here, it becomes significantly more complex when state must be managed
or when exactly-once semantics is required without an idempotent writer.
In these cases, [Apache Flink](https://flink.apache.org/) with the 
[Pravega connectors](https://github.com/pravega/flink-connectors) provides a framework
to greatly simplify application development.
