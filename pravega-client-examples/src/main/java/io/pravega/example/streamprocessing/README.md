# Pravega Stream Processing Example


# Overview

The examples in this directory are intended to illustrate
how exactly-once semantics can be achieved with Pravega.
To make it clear to the reader, the entirety of the stream processor is defined
in a single source file of under 500 lines.
In particular, these illustrative examples do not use Apache Flink.

As one reads and understands this example exactly-once processor, they should
understand that enterprise-grade stream processing at scale requires
a sophisticated stream processing system like Apache Flink.
In addition to providing exactly-once processing, it also handles
windowing, aggregations, event time processing, state management, and a lot more.

These examples include:

- [EventGenerator](EventGenerator.java):
  This application generates new events every 1 second
  and writes them to a Pravega stream (referred to as stream1).

- [ExactlyOnceMultithreadedProcessor](ExactlyOnceMultithreadedProcessor.java):
  This application continuously reads events from stream1, performs a stateful computation
  to generate output events, and writes the output event to another
  Pravega stream (referred to as stream2).
  It uses the exactly-once algorithms described below.

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
   ```
   cd pravega-samples
   ./gradlew pravega-client-examples:startEventGenerator
   ```

   All parameters are provided as environment variables.
   You can either set them in your shell (`export PRAVEGA_SCOPE=examples`) or use the below syntax.

   If you are using a non-local Pravega instance, specify the controller as follows:
   ```
   PRAVEGA_CONTROLLER=tcp://pravega.example.com:9090 ./gradlew pravega-client-examples:startEventGenerator
   ```

   Multiple parameters can be specified as follows.
   ```
   PRAVEGA_SCOPE=examples PRAVEGA_CONTROLLER=tcp://localhost:9090 ./gradlew pravega-client-examples:startEventGenerator
   ```

   See [Parameters.java](Parameters.java) for available parameters.

- In another window, start the stream processor:
  ```
  ./gradlew pravega-client-examples:startExactlyOnceMultithreadedProcessor
  ```

- In another window, start the event debug sink:
  ```
  ./gradlew pravega-client-examples:startEventDebugSink
  ```

- Note: The [ExactlyOnceMultithreadedProcessor](ExactlyOnceMultithreadedProcessor.java)
  will automatically restart from the latest checkpoint.
  However, if Pravega streams are truncated or deleted, or checkpoint files in
  /tmp/checkpoints are deleted or otherwise bad, you may need to start over from
  a clean system. To do so, follow these steps:
    - Stop the event generator, stream processor, and event debug sink.
    - Delete the contents of /tmp/checkpoints.
    - Use a new scope (PRAVEGA_SCOPE) or streams (PRAVEGA_STREAM_1 and PRAVEGA_STREAM_2).


# Achieving At-Least-Once Semantics

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


# Achieving Exactly-Once Semantics

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
