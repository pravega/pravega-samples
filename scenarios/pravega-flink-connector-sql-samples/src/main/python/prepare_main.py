"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.table_environment import StreamTableEnvironment


def prepare_main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql(f"""CREATE TABLE file_source (
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
    destLocationServiceZone STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/table_data.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
)""")

    t_env.execute_sql("""CREATE TABLE source (
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
    destLocationServiceZone STRING
) WITH (
    'connector' = 'pravega',
    'controller-uri' = 'tcp://127.0.0.1:9090',
    'scope' = 'taxi',
    'scan.streams' = 'trip',
    'sink.stream' = 'trip',
    'format' = 'json'
)""")

    t_env.execute_sql("""INSERT INTO source SELECT * FROM file_source""")


if __name__ == '__main__':
    prepare_main()
