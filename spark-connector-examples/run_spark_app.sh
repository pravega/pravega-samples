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

# Run Pravega Java/Scala/Python Spark applications locally.

set -ex

USE_SDP=${USE_SDP:-0}
USE_IN_PROCESS_SPARK=${USE_IN_PROCESS_SPARK:-1}

KEY_CLOACK_CREDENTIALS_VERSION=${KEY_CLOACK_CREDENTIALS_VERSION:-0.12.0}
SPARK_CONNECTOR_VERSION=${SPARK_CONNECTOR_VERSION:-0.12.0}
PACKAGES="--packages \
io.pravega:pravega-connectors-spark-3_2.12:${SPARK_CONNECTOR_VERSION},\
io.pravega:pravega-keycloak-client:${KEY_CLOACK_CREDENTIALS_VERSION}"

export PRAVEGA_CONTROLLER=${PRAVEGA_CONTROLLER:-tcp://localhost:9090}
export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}
export PATH=$PATH:$HOME/spark/current/bin

if [ $USE_IN_PROCESS_SPARK == "1" ]
then
    SPARK_MASTER="local[2]"
else
    SPARK_MASTER="spark://$(hostname):7077"
fi

spark-submit \
--master $SPARK_MASTER \
--driver-memory 4g \
--executor-memory 4g \
--total-executor-cores 1 \
$PACKAGES \
$* |& tee -a app.log
