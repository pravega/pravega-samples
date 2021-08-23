"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.table.window import Slide

from common import create_table_ddl


def popular_taxi_vendor():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql(
        create_table_ddl(
            "WATERMARK FOR pickupTime AS pickupTime - INTERVAL '30' SECONDS"))
    taxi_ride = t_env.from_path('TaxiRide')
    popular_rides = taxi_ride.select(taxi_ride.vendorId, taxi_ride.pickupTime) \
        .window(Slide.over('15.minutes').every('5.minutes').on(taxi_ride.pickupTime).alias('w')) \
        .group_by(taxi_ride.vendorId, col('w')) \
        .select(taxi_ride.vendorId, \
                col('w').start.alias('start'), \
                col('w').end.alias('end'), \
                taxi_ride.vendorId.count.alias('cnt'))

    t_env.to_append_stream(
        popular_rides,
        Types.ROW_NAMED(['vendorId', 'start', 'end', 'cnt'], [
            Types.INT(),
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.LONG()
        ])).print()

    env.execute('Popular-Taxi-Vendor')


if __name__ == '__main__':
    popular_taxi_vendor()
