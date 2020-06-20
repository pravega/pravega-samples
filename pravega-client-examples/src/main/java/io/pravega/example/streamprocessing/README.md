# Pravega Stream Processing Example

# Overview

The examples in this directory are intended to illustrate
how at-least-once and exactly-once semantics can be achieved with Pravega.
To make it clear to the reader, the entirety of the stream processor is defined
within this package with minimal dependencies.
In particular, these illustrative examples do *not* use Apache Flink.
In addition to exactly-once semantics, Apache Flink provides
windowing, aggregations, event time processing, and stateful processing.

These examples include:

- [EventGenerator](EventGenerator.java):
  This application generates new events every 1 second
  and writes them to a Pravega stream (referred to as stream1).

- [AtLeastOnceApp](AtLeastOnceApp.java):
  This application demonstrates reading events from a Pravega stream (stream1), processing each event,
  and writing each output event to another Pravega stream (stream2).
  It guarantees that each event is processed at least once, using the algorithms described below.
  If multiple instances of this application are executed using the same readerGroupName parameter,
  each instance will get a distinct subset of events, providing load balancing and redundancy.
  The user-defined processing function must be stateless.
  
  There is no direct dependency on other systems, except for Pravega.
  In particular, this application does not *directly* use a distributed file system, nor ZooKeeper.
  Coordination between processes occurs only through Pravega streams such as
  Reader Groups and State Synchronizers.

- [EventDebugSink](EventDebugSink.java):
  This application reads events from stream2 and displays them
  on the console.

# How to Run

- Enable INFO level logging by editing the file [logback.xml](../../../../../resources/logback.xml).
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

   See [Parameters.java](Parameters.java) for available appConfiguration.

- In another window, start one or more instances of the stream processor.
  The `runAtLeastOnceApp.sh` can be used to run multiple instances concurrently.
  ```shell script
  cd ~/pravega-samples/pravega-client-examples
  ./runAtLeastOnceApp.sh 2
  ```

- In another window, start the event debug sink:
  ```shell script
  ./gradlew pravega-client-examples:startEventDebugSink
  ```

# (OBSOLETE) Achieving At-Least-Once Semantics

Pravega has a sophisticated concept called a 
[Reader Group](http://pravega.io/docs/latest/reader-group-design/).

The complete state of a Reader Group is maintained by each reader.
Each reader reads updates from the Reader Group stream. 

The current position in a Pravega stream is defined by a stream cut.
A stream cut is essentially a mapping from segment numbers to byte offsets
within that segment.
However, because the number of concurrent segments can dynamically
increase and decrease for a single stream, it is not possible for
a Pravega application to maintain this mapping by itself.
To achieve at-least-once semantics with Pravega, use the algorithms in this section.

In the algorithms below, one process in a distributed system will be designated
by the user as a master. There can be any number of worker processes.

## Checkpoint Algorithm

- Master
    - Create a checkpoint name as a new random UUID.
    - Create a directory in persistent storage (e.g. NFS/HDFS) to store information
      about this checkpoint.
      For instance: /checkpoints/5ef9b301-2af1-4320-8028-c4cef9f39aca
    - Initiate a checkpoint on the reader group used by the workers.
    - Wait for all workers to complete handling the checkpoint.
    - After waiting, the Pravega checkpoint object, including the
      stream cuts, will be available.
    - Write the Pravega checkpoint object to the checkpoint directory.
    - Atomically update the file /checkpoints/latest to reference the
      checkpoint name.

- Worker
    - Loop:
        - Read the next event from the reader group.
          Immediately after a checkpoint event, reading the next event
          will notify the master that it has finished handling the checkpoint.
        - If this is a checkpoint event:
            - Write worker state to the checkpoint directory.
        - If this is a normal event:
            - Process the event and write output event(s).

## Restart/Recovery Algorithm

- Master:
    - Read the file /checkpoints/latest to determine the directory
      containing the latest successful checkpoint.
    - Read the Pravega checkpoint object to determine the stream cut
      at the last checkpoint.
    - Create a reader group that starts reading from this stream cut.
    - Launch worker processes, passing the name of the checkpoint and
      the reader group.

- Worker:
    - Load worker state from the checkpoint directory.
    - Create a Pravega reader that reads from the reader group.
    - Process events as usual.


# (OBSOLETE) Achieving Exactly-Once Semantics

Exactly-once semantics can achieved by starting with at-least-once
semantics and adding write idempotence.
In Pravega, write idempotence can be achieved by using Pravega
transactions.

Essentially, the at-least-once algorithm is modified in the following ways:

- Each workers start its own Pravega transaction.
  All output events are written to the transaction.
  If a worker writes to multiple Pravega streams, it would use
  a different transaction for each stream.

- When a worker receives a checkpoint event, it flushes
  (but does not commit) the transaction
  and adds the transaction ID to the checkpoint directory.

- When all workers complete handling of the checkpoint, the master
  updates the checkpoint directory as before and then
  commits all transactions referenced in the checkpoint directory.

- Upon restart, any transactions referenced in the checkpoint
  directory are committed if they have not yet been committed.

  Note: If any transactions fail to commit (usually due to a
  transaction timeout), the stream processor will terminate
  as it cannot guarantee exactly-once semantics.
  To maintain correctness, recovery
  from such a situation will usually require deleting the output stream
  and recreating it from the beginning.
  To avoid such problems, ensure that failed stream processors
  are restarted before transactions timeout.
  See also
  [EventWriterConfig.java](https://github.com/pravega/pravega/blob/r0.4/client/src/main/java/io/pravega/client/stream/EventWriterConfig.java#L27).


See [ExactlyOnceMultithreadedProcessor.java](ExactlyOnceMultithreadedProcessor.java)
for details.
