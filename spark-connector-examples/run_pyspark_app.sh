#!/usr/bin/env bash
# Run Pravega PySpark applications locally.

set -ex

export PYSPARK_PYTHON=${PYSPARK_PYTHON:-$(which python)}

./run_spark_app.sh $*
