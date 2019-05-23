#!/usr/bin/env bash
# Run Pravega Java/Scala/Python Spark applications locally.

set -ex

USE_NAUTILUS=${USE_NAUTILUS:-0}
USE_IN_PROCESS_SPARK=${USE_IN_PROCESS_SPARK:-1}

KEY_CLOACK_CREDENTIALS_VERSION=${KEY_CLOACK_CREDENTIALS_VERSION:-0.4.0-2030.d99411b-0.0.1-020.26736d2}
SPARK_CONNECTOR_VERSION=${SPARK_CONNECTOR_VERSION:-0.4.0-SNAPSHOT}

if [ $USE_NAUTILUS == "1" ]
then
    PACKAGES="--packages \
io.pravega:pravega-connectors-spark:${SPARK_CONNECTOR_VERSION},\
io.pravega:pravega-keycloak-credentials:${KEY_CLOACK_CREDENTIALS_VERSION}"
    export PRAVEGA_CONTROLLER=${PRAVEGA_CONTROLLER:-tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090}
else
    CONNECTOR_JAR="${HOME}/.m2/repository/io/pravega/pravega-connectors-spark/${SPARK_CONNECTOR_VERSION}/pravega-connectors-spark-${SPARK_CONNECTOR_VERSION}.jar"
    ls -lh "${CONNECTOR_JAR}"
    PACKAGES="--jars ${CONNECTOR_JAR}"
    export PRAVEGA_CONTROLLER=${PRAVEGA_CONTROLLER:-tcp://localhost:9090}
fi

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
$PACKAGES \
$* |& tee -a app.log
