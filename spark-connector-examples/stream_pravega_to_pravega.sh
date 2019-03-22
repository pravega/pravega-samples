#!/usr/bin/env bash

set -ex

export PRAVEGA_CONTROLLER=tcp://${HOST_IP:-127.0.0.1}:9090
export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}

#export PYSPARK_PYTHON=$HOME/.conda/envs/pravega-samples/bin/python

master=local[8]
#master=spark://localhost:7077

spark-submit \
--master $master \
--packages io.pravega:pravega-connectors-spark:0.4.0-SNAPSHOT \
src/main/python/stream_pravega_to_pravega.py
