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

# Run Pravega PySpark applications locally.

set -ex

export PYSPARK_PYTHON=${PYSPARK_PYTHON:-$(which python)}

./run_spark_app.sh $*
