#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
cd ${ROOT_DIR}
./gradlew pravega-client-examples:distTar
cd pravega-client-examples/build/distributions
tar -xf pravega-client-examples-0.10.0-SNAPSHOT.tar
cd pravega-client-examples-0.10.0-SNAPSHOT

NUM_INSTANCES=${1:-1}

for i in $(seq -w 01 $NUM_INSTANCES); do
  LOG_FILE=/tmp/atLeastOnceApp-$i.log
  echo Logging to ${LOG_FILE}
  INSTANCE_ID=$i bin/atLeastOnceApp >& ${LOG_FILE} &
done

echo Instances have been started as background jobs.

tail -f /tmp/atLeastOnceApp-*.log
