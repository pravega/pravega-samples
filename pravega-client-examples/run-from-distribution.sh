#!/usr/bin/env bash
set -ex
./gradlew pravega-client-examples:build
cd pravega-client-examples/build/distributions
tar -xf pravega-client-examples-0.7.0.tar
cd pravega-client-examples-0.7.0
$*
