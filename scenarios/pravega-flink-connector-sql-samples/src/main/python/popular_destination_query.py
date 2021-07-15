from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.table_environment import StreamTableEnvironment

from common import create_table_ddl, args


def popular_destination_query():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql(
        create_table_ddl(
            "WATERMARK FOR pickupTime AS pickupTime - INTERVAL '30' SECONDS"))

    query = f"""SELECT 
    destLocationId, wstart, wend, cnt 
FROM 
    (SELECT 
        destLocationId, 
        HOP_START(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart, 
        HOP_END(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend, 
        COUNT(destLocationId) AS cnt 
    FROM
        (SELECT 
            pickupTime, 
            destLocationId 
        FROM TaxiRide) 
    GROUP BY
        destLocationId, HOP(pickupTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
    )
WHERE cnt > {args.threshold}
"""

    results = t_env.sql_query(query)

    t_env.to_append_stream(
        results,
        Types.ROW_NAMED(['destLocationId', 'wstart', 'wend', 'cnt'], [
            Types.INT(),
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.LONG()
        ])).print()

    env.execute('Popular-Destination')


if __name__ == '__main__':
    popular_destination_query()
