from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")

spark = (SparkSession
         .builder
         .getOrCreate()
         )

df = (spark
    .read
    .format("pravega") 
    .option("controller", controller) 
    .option("scope", scope) 
    .option("stream", "batchstream1")
    .load()
    .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
 )

event_count = df.count()

df.show(1000, truncate=False)

print(f"Number of events in Pravega stream: {event_count}")
print("Done.")
