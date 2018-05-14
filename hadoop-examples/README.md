<!--
Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# hadoop-connectors examples
Examples of Hadoop Connectors for Pravega.

Description
-----------
These examples give you some basic ideas how to use hadoop-connectors for pravega.

Build
-------
(Most of these steps will be removed when GAed)
### build hadoop connectors
```
git clone --recurse-submodules https://github.com/pravega/hadoop-connectors.git
cd hadoop-connectors
gradle install

# start pravega whose version matches hadoop-connectors
cd pravega
./gradlew startStandalone
```

## build hadoop connectors examples
```
open another terminal and goto ~/pravega-samples
cd hadoop-examples
gradle build
```

Run Examples on Local Machine
---


Hadoop (verified with Hadoop 2.8.3 on Ubuntu 16.04)
```
1. setup and start hdfs

2. set env variables
   export HDFS=hdfs://<hdfs_ip_and_port> # e.g. hdfs://192.168.0.188:9000
   export HADOOP_EXAMPLES_JAR=<pravega-hadoop-examples-0.3.0-SNAPSHOT-all.jar location> # e.g. ./build/libs/pravega-hadoop-examples-0.3.0-SNAPSHOT-all.jar
   export HADOOP_EXAMPLES_INPUT_DUMMY=${HDFS}/tmp/hadoop_examples_input_dummy
   export HADOOP_EXAMPLES_OUTPUT=${HDFS}/tmp/hadoop_examples_output
   export PRAVEGA_URI=tcp://<pravega_controller_ip_and_port> # e.g. tcp://192.168.0.188:9090
   export PRAVEGA_SCOPE=<scope_name>   # e.g. myScope
   export PRAVEGA_STREAM=<stream_name> # e.g. myStream
   export CMD=wordcount # so far, can also try wordmean and wordmedian

3. make sure below dirs are empty
   hadoop fs -rmr ${HADOOP_EXAMPLES_INPUT_DUMMY}
   hadoop fs -rmr ${HADOOP_EXAMPLES_OUTPUT}

4. generate words into pravega
   hadoop jar ${HADOOP_EXAMPLES_JAR} randomtextwriter -D mapreduce.randomtextwriter.totalbytes=32000 ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}

5. run hadoop command
   hadoop jar ${HADOOP_EXAMPLES_JAR} ${CMD} ${HADOOP_EXAMPLES_INPUT_DUMMY} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM} ${HADOOP_EXAMPLES_OUTPUT}
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
   file:///<full_path_of_pravega-samples>/hadoop-examples/HiBench/report/wordcount/hadoop/monitor.html
```


You can also use hadoop-connectors on Spark
```
Spark (verified with Spark 2.2.1 on Ubuntu 16.04)
   spark-submit --class io.pravega.examples.spark.WordCount ${HADOOP_EXAMPLES_JAR} ${PRAVEGA_URI} ${PRAVEGA_SCOPE} ${PRAVEGA_STREAM}
```
