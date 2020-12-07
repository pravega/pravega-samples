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

set -ex

../gradlew build

master=local[8]
#master=spark://localhost:7077

spark-submit \
--master $master \
--packages io.pravega:pravega-connectors-spark:0.4.0-SNAPSHOT \
--class io.pravega.example.spark.StreamPravegaToConsole \
build/libs/pravega-spark-connector-examples-0.5.0-SNAPSHOT.jar
