#!/usr/bin/env bash

set -ex

../gradlew build

master=local[8]
#master=spark://localhost:7077

spark-submit \
--master $master \
--packages io.pravega:pravega-connectors-spark:0.4.0-SNAPSHOT \
--class io.pravega.example.spark.StreamPravegaToPravega \
build/libs/pravega-spark-connector-examples-0.5.0-SNAPSHOT.jar
