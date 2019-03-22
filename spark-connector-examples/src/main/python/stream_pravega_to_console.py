from pyspark.sql import SparkSession
import os

spark = SparkSession \
    .builder \
    .appName("stream_pravega_to_console") \
    .getOrCreate()

controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
scope = os.getenv('PRAVEGA_SCOPE', 'examples')

spark \
    .readStream \
    .format("pravega") \
    .option("controller", controller) \
    .option("scope", scope) \
    .option("stream", "streamprocessing1") \
    .load() \
    .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset") \
    .writeStream \
    .trigger(processingTime='3 seconds') \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark-checkpoints-stream_pravega_to_console") \
    .start() \
    .awaitTermination()
