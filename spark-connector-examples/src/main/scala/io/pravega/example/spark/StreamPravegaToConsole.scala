/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StreamPravegaToConsole {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("StreamPravegaToConsole")
      .getOrCreate()

    val scope = sys.env.getOrElse("PRAVEGA_SCOPE", "examples")
    val controller = sys.env.getOrElse("PRAVEGA_CONTROLLER", "tcp://127.0.0.1:9090")

    spark
      .readStream
      .format("pravega")
      .option("controller", controller)
      .option("scope", scope)
      .option("stream", "streamprocessing1")
      // If there is no checkpoint, start at the earliest event.
      .option("start_stream_cut", "earliest")
      .load()
      .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
      .writeStream
      .trigger(Trigger.ProcessingTime(3000))
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/tmp/spark-checkpoints-StreamPravegaToConsole")
      .start()
      .awaitTermination()
  }
}
