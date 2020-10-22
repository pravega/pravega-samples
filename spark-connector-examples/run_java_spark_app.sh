#!/usr/bin/env bash
# Run Pravega Java/Scala Spark applications locally.

set -ex

EXAMPLES_VESION=${EXAMPLES_VERSION:-0.8.0-SNAPSHOT}

CLASS_TO_RUN=$1
shift

../gradlew build

./run_spark_app.sh \
--class $CLASS_TO_RUN \
build/libs/pravega-spark-connector-examples-${EXAMPLES_VESION}.jar
$*
