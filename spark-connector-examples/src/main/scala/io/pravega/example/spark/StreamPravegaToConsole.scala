package io.pravega.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StreamPravegaToConsole {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("StreamPravegaToConsole")
      .getOrCreate()

    val scope = "examples"
    val controller = "tcp://127.0.0.1:9090"

    spark
      .readStream
      .format("pravega")
      .option("controller", controller)
      .option("scope", scope)
      .option("stream", "streamprocessing1")
      .load()
      .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
      .writeStream
      .trigger(Trigger.ProcessingTime(3000))
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "/tmp/spark-checkpoints-StreamPravegaToConsole")
      .start()
      .awaitTermination()
  }
}
