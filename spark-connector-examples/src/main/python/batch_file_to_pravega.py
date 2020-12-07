#  Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0

from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")
filename = "sample_data.json"

spark = (SparkSession
    .builder 
    .getOrCreate()
)

df = (spark
    .read
    .format("json")
    .load(filename)
    .selectExpr(
        "to_json(struct(*)) as event",  # Re-encode all the fields as a JSON string
        "key as routing_key"            # Optional routing key
    )
)

df.show(20, truncate=False)

(df
    .write
    .format("pravega")
    .option("controller", controller)
    .option("scope", scope)
    .option("stream", "batchstream1")
    .option("default_num_segments", "5")
    .save()
)

print("Done.")
