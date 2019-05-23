from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")

spark = (SparkSession
         .builder
         .getOrCreate()
         )

(spark 
    .readStream 
    .format("pravega") 
    .option("controller", controller) 
    .option("scope", scope) 
    .option("stream", "streamprocessing1")
    # If there is no checkpoint, start at the earliest event.
    .option("start_stream_cut", "earliest")
    # Do not read past the last event as of the job start time. The job will continue to run but will not receive any new records.
    .option("end_stream_cut", "latest")
    .load()
    .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
    .writeStream 
    .trigger(processingTime="3 seconds") 
    .outputMode("append") 
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints-stream_bounded_pravega_to_console")
    .start() 
    .awaitTermination()
 )
