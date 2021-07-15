from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.table.window import Tumble

from common import create_table_ddl


def max_travellers_per_destination():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql(create_table_ddl(
            "WATERMARK FOR dropOffTime AS dropOffTime - INTERVAL '30' SECONDS"))
    taxi_ride = t_env.from_path('TaxiRide')
    no_of_travelers_per_dest = taxi_ride \
        .select(taxi_ride.passengerCount, taxi_ride.dropOffTime, taxi_ride.destLocationZone) \
        .window(Tumble().over('1.hour').on(taxi_ride.dropOffTime).alias('w')) \
        .group_by(taxi_ride.destLocationZone, col('w')) \
        .select(taxi_ride.destLocationZone, \
                col('w').start.alias('start'), \
                col('w').end.alias('end'), \
                taxi_ride.passengerCount.count.alias('cnt'))

    t_env.to_append_stream(
        no_of_travelers_per_dest,
        Types.ROW_NAMED(['destLocationZone', 'start', 'end', 'cnt'],
            [Types.INT(),
             Types.SQL_TIMESTAMP(),
             Types.SQL_TIMESTAMP(),
             Types.LONG()])).print()


if __name__ == '__main__':
    max_travellers_per_destination()
