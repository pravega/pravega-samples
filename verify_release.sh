#!/bin/bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

# This script contains code for validating a given Pravega release.

# 1. Verify maven dependencies..

# By pointing the `pravegaVersion` property to the target, we can check whether we can download the maven dependencies.
RELEASE_NAME=${1:-"v1.0"}
./gradlew check -PpravegaVersion=$RELEASE_NAME --refresh-dependencies

DOCKER_IMAGE_NAME=${2:-"1.0"}
docker pull pravega/pravega:$DOCKER_IMAGE_NAME
