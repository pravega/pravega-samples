#!/usr/bin/env bash
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

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
