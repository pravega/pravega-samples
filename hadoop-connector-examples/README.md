<!--
Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Hadoop Connector Examples for Pravega
Code examples toe give you some basic ideas how to use hadoop-connectors for Pravega.

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
3. Apache Hadoop running

---

# Examples Catalog

## Word Count

Hadoop (verified with Hadoop 2.8.3 on Ubuntu 16.04)

### Execution

```
1. setup and start hdfs as well as pravega

2. set env variables
   export HDFS=hdfs://<hdfs_ip_and_port> # e.g. hdfs://192.168.0.188:9000
   export HADOOP_EXAMPLES_JAR=<pravega-hadoop-examples-x.y.z.jar location> # e.g. ./build/libs/pravega-hadoop-examples-0.4.0-SNAPSHOT-all.jar
   export HADOOP_EXAMPLES_INPUT_DUMMY=${HDFS}/tmp/hadoop_examples_input_dummy
   export HADOOP_EXAMPLES_OUTPUT=${HDFS}/tmp/hadoop_examples_output
   export PRAVEGA_URI=tcp://<pravega_controller_ip_and_port> # e.g. tcp://192.168.0.188:9090
   export PRAVEGA_SCOPE=<scope_name>   # e.g. myScope
   export PRAVEGA_STREAM=<stream_name> # e.g. myStream
   export CMD=wordcount # so far, can also try wordmean and wordmedian

3. make sure input dir is empty
   hadoop fs -rmr ${HADOOP_EXAMPLES_INPUT_DUMMY}

4. run hadoop commands

   4.1 to process all events in the stream
      // generate new events in pravega
      hadoop jar ${HADOOP_EXAMPLES_JAR} randomtextwriter -D mapreduce.randomtextwriter.totalbytes=32000 ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}

      hadoop fs -rmr ${HADOOP_EXAMPLES_OUTPUT}
      hadoop jar ${HADOOP_EXAMPLES_JAR} ${CMD} ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM} ${HADOOP_EXAMPLES_OUTPUT}

   4.2 to only process events in the stream cut (http://pravega.io/docs/latest/streamcuts/)
      // find last line of output of last command, and assign positions to env variable
      //   e.g.
      //   18/07/25 14:04:39 INFO wordcount.WordCount: End positions of stream cut myScope/myStream: '[[{"scope":"myScope","streamName":"myStream","segmentId":0},48025],[{"scope":"myScope","streamName":"myStream","segmentId":2},41564]]'
      export POSITIONS_1='[[{"scope":"myScope","streamName":"myStream","segmentId":0},48025],[{"scope":"myScope","streamName":"myStream","segmentId":2},41564]]'

      // generate new events in pravega
      hadoop jar ${HADOOP_EXAMPLES_JAR} randomtextwriter -D mapreduce.randomtextwriter.totalbytes=32000 ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}

      // process new generated events: (POSITIONS_1, latest]
      hadoop fs -rmr ${HADOOP_EXAMPLES_OUTPUT}
      hadoop jar ${HADOOP_EXAMPLES_JAR} ${CMD} ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM} ${HADOOP_EXAMPLES_OUTPUT} ${POSITIONS_1}

      // similarly get current end positions from last line
      export POSITIONS_2='[[{"scope":"myScope","streamName":"myStream","segmentId":0},63774],[{"scope":"myScope","streamName":"myStream","segmentId":2},53121]]'

      // generate new events in pravega
      hadoop jar ${HADOOP_EXAMPLES_JAR} randomtextwriter -D mapreduce.randomtextwriter.totalbytes=32000 ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}

      // process the events between two positions: (POSITIONS_1, POSITIONS_2]
      hadoop fs -rmr ${HADOOP_EXAMPLES_OUTPUT}
      hadoop jar ${HADOOP_EXAMPLES_JAR} ${CMD} ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM} ${HADOOP_EXAMPLES_OUTPUT} ${POSITIONS_1} ${POSITIONS_2}
```


Additionally, you can run WordCount program (more will be coming soon) on top of [HiBench](https://github.com/intel-hadoop/HiBench)
```
0. set same env variables as previous section, and
   export HADOOP_HOME=<hadoop_home_dir>  # e.g. /services/hadoop-2.8.3
   export HDFS=hdfs://<hdfs_ip_and_port> # e.g. hdfs://192.168.0.188:9000
   export INPUT_HDFS="${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}"

1. fetch/build/patch HiBench (make sure mvn is installed)
   gradle wcHiBench

2. prepare testing data
   ./HiBench/bin/workloads/micro/wordcount/prepare/prepare.sh

3. run
   ./HiBench/bin/workloads/micro/wordcount/hadoop/run.sh

4. check report
   file:///<full_path_of_pravega-samples>/hadoop-connector-examples/HiBench/report/wordcount/hadoop/monitor.html
```


You can also use hadoop-connectors on Spark
```
Spark (verified with Spark 2.2.1 on Ubuntu 16.04)
   spark-submit --class io.pravega.examples.spark.WordCount ${HADOOP_EXAMPLES_JAR} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}
```

## Terasort

Hadoop (verified with Hadoop 2.8.1 and 3.1.1 on Ubuntu 16.04)

### Execution

```
1. setup and start hdfs as well as pravega

2. set env variables
   export HDFS=hdfs://<hdfs_ip_and_port> # e.g. hdfs://192.168.0.188:9000
   export HADOOP_EXAMPLES_JAR=<pravega-hadoop-examples-x.y.z-SNAPSHOT-all.jar location> # e.g. ./build/libs/pravega-hadoop-examples-0.4.0-SNAPSHOT-all.jar
   export HADOOP_EXAMPLES_INPUT_DUMMY=${HDFS}/tmp/hadoop_examples_input_dummy
   export HADOOP_EXAMPLES_OUTPUT=${HDFS}/tmp/hadoop_examples_output
   export PRAVEGA_URI=tcp://<pravega_controller_ip_and_port> # e.g. tcp://192.168.0.188:9090
   export PRAVEGA_SCOPE=<scope_name>   # e.g. myScope
   export PRAVEGA_INPUT_STREAM=<stream_name> # e.g. intStream
   export PRAVEGA_OUTPUT_STREAM=<stream_name> # e.g. outStream
   export PRAVEGA_OUTPUT_STREAM_PREFIX=<stream_name_prefix> # e.g. sortedStream
   export CMD=wordcount # so far, can also try wordmean and wordmedian

3. make sure input and output dir are empty
   hdfs dfs -rmr ${HADOOP_EXAMPLES_INPUT_DUMMY}
   hdfs dfs -rmr ${HADOOP_EXAMPLES_OUTPUT}

4. run hadoop commands

   4.1 to generate events into a Pravega stream
      hadoop jar ${HADOOP_EXAMPLES_JAR} teragen [1m|1b|1t] ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}
      e.g. hadoop jar $HADOOP_EXAMPLES_JAR teragen 10 hdfs://localhost:8020/pravega/input tcp://localhost:9090 myScope inputStream

   4.2 to sort all the events from the input stream and put sorted events into output stream(s)
      hadoop jar ${HADOOP_EXAMPLES_JAR} terasort ${HADOOP_EXAMPLES_INPUT_DUMMY} ${HADOOP_EXAMPLES_OUTPUT} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_INPUT_STREAM} ${PRAVEGA_OUTPUT_STREAM_PREFIX}
      e.g. hadoop jar $HADOOP_EXAMPLES_JAR terasort -D mapreduce.job.maps=3 -D mapreduce.job.reduces=3 hdfs://localhost:8020/pravega/input hdfs://localhost:8020/pravega/output tcp://localhost:9090 myScope inputStream outputStream

   4.3 to dump events from output stream(s) into corresponding hdfs file(s), in order to manually verify whether terasort is correctly done 
      hadoop jar ${HADOOP_EXAMPLES_JAR} terastreamvalidate ${HADOOP_EXAMPLES_INPUT_DUMMY} ${HADOOP_EXAMPLES_OUTPUT} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_OUTPUT_STREAM_PREFIX}[0-9]+
      e.g. (in case of 3 reducers)
      hadoop jar $HADOOP_EXAMPLES_JAR terastreamvalidate hdfs://localhost:8020/pravega/input hdfs://localhost:8020/pravega/validate/output0 tcp://localhost:9090 myScope outputStream0
      hadoop jar $HADOOP_EXAMPLES_JAR terastreamvalidate hdfs://localhost:8020/pravega/input hdfs://localhost:8020/pravega/validate/output1 tcp://localhost:9090 myScope outputStream1
      hadoop jar $HADOOP_EXAMPLES_JAR terastreamvalidate hdfs://localhost:8020/pravega/input hdfs://localhost:8020/pravega/validate/output2 tcp://localhost:9090 myScope outputStream2
