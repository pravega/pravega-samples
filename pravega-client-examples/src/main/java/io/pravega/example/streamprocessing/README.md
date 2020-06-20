# Pravega Stream Processing Example

# Overview

The examples in this directory are intended to illustrate
how at-least-once semantics can be achieved with Pravega.
These show how to use Pravega directly, without relying on an external framework such as Apache Flink.

These examples include:

- [EventGenerator](EventGenerator.java):
  This application generates new events every 1 second
  and writes them to a Pravega stream (referred to as stream1).

- [AtLeastOnceApp](AtLeastOnceApp.java):
  This application demonstrates reading events from a Pravega stream (stream1), processing each event,
  and writing each output event to another Pravega stream (stream2).
  It guarantees that each event is processed at least once, even in the presence of failures.
  If multiple instances of this application are executed using the same reader group name,
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

- (Optional) Enable INFO level logging by editing the file [logback.xml](../../../../../resources/logback.xml).
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

# Achieving At-Least-Once Semantics

Pravega has a sophisticated concept called a 
[Reader Group](http://pravega.io/docs/latest/reader-group-design/).

The complete state of a Reader Group is maintained by each reader.
Each reader reads updates from the Reader Group stream. 

TODO: Finish this section.
