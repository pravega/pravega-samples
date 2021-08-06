"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""
import argparse

DEFAULT_SCOPE = 'taxi'
DEFAULT_STREAM = 'trip'
DEFAULT_CONTROLLER_URI = "127.0.0.1:9090"
DEFAULT_POPULAR_DEST_THRESHOLD = 20


class Arguments():
    scope: str
    stream: str
    controller_uri: str
    threshold: int


def get_arguments() -> Arguments:
    parser = argparse.ArgumentParser(description='Change the pravega properties.')
    parser.add_argument('--scope', default=DEFAULT_SCOPE, type=str, nargs='?')
    parser.add_argument('--stream',
                        default=DEFAULT_STREAM,
                        type=str,
                        nargs='?')
    parser.add_argument('--controller-uri',
                        default=DEFAULT_CONTROLLER_URI,
                        type=str,
                        nargs='?')
    parser.add_argument('--threshold',
                        default=DEFAULT_POPULAR_DEST_THRESHOLD,
                        type=int,
                        nargs='?')
    args = Arguments()
    parser.parse_known_args(namespace=args)
    return args


args = get_arguments()


def create_table_ddl(watermark_ddl: str) -> str:
    return f"""CREATE TABLE TaxiRide (
    rideId INT,
    vendorId INT,
    pickupTime TIMESTAMP(3),
    dropOffTime TIMESTAMP(3),
    passengerCount INT,
    tripDistance FLOAT,
    startLocationId INT,
    destLocationId INT,
    startLocationBorough STRING,
    startLocationZone STRING,
    startLocationServiceZone STRING,
    destLocationBorough STRING,
    destLocationZone STRING,
    destLocationServiceZone STRING,
    {watermark_ddl}
) with (
    'connector' = 'pravega',
    'controller-uri' = 'tcp://{args.controller_uri}',
    'scope' = '{args.scope}',
    'scan.execution.type' = 'streaming',
    'scan.streams' = '{args.stream}',
    'format' = 'json'
)"""
