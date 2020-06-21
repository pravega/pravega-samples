#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
cd ${ROOT_DIR}
./gradlew pravega-client-examples:build
cd pravega-client-examples/build/distributions
tar -xf pravega-client-examples-0.7.0.tar
cd pravega-client-examples-0.7.0

NUM_INSTANCES=${1:-1}

for i in $(seq -w 01 $NUM_INSTANCES); do
  INSTANCE_ID=$i bin/atLeastOnceApp >& /tmp/atLeastOnceApp-$i.log &
done
