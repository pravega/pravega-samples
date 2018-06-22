#!/bin/bash
#
# sample script to create scope and stream using Pravega REST API
#
host=localhost
port=9091
scope=myscope
stream=apacheaccess
curl -v -H "Content-Type: application/json" $host:${port}/v1/scopes 
-d '{
    "scopeName": "'${scope}'"
}'

curl -v -H "Content-Type: application/json" $host:${port}/v1/scopes/${scope}/streams \
-d '{
    "streamName": "'${stream}'",
    "scopeName": "'${scope}'",
    "scalingPolicy":{
        "type": "FIXED_NUM_SEGMENTS",
        "minSegments": 1
    }
}'
