/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.turbineheatprocessor

import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.connectors.flink.{FlinkPravegaReader, PravegaConfig}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import scala.math._

/**
  * Sample Flink Streaming program to process temperature data produced by the
  * accompanying TurbineHeatSensor app.
  */
object TurbineHeatProcessorScala {

  /**
    * A raw sensor event.
    * @param timestamp the timestamp indicating when the sensor was read (in event time).
    * @param sensorId the sensor ID
    * @param location the sensor location
    * @param temp the temperature reading
    */
  case class SensorEvent(timestamp: Long, sensorId: Int, location: String, temp: Float)

  /**
    * An aggregate of sensor events within a one-day time window.
    *
    * @param startTime the start time of the window (i.e. the first event in that window).
    * @param sensorId the sensor ID
    * @param location the sensor location
    * @param tempRange the temperature range observed in the window
    */
  case class SensorAggregate(startTime: Long, sensorId: Int, location: String, tempRange: (Float,Float))

  object SensorAggregate extends AggregateFunction[SensorEvent, SensorAggregate, SensorAggregate]{
    val nothing = null.asInstanceOf[SensorAggregate]

    override def createAccumulator(): SensorAggregate = nothing

    override def add(evt: SensorEvent, acc: SensorAggregate): SensorAggregate = {
      acc match {
        case null =>
          SensorAggregate(evt.timestamp, evt.sensorId, evt.location, (evt.temp, evt.temp))
        case _ =>
          SensorAggregate(acc.startTime, evt.sensorId, evt.location, (min(evt.temp, acc.tempRange._1), max(acc.tempRange._2, evt.temp)))
      }
    }

    override def getResult(accumulator: SensorAggregate): SensorAggregate = accumulator

    // This will not be called in time window
    override def merge(a: SensorAggregate, b: SensorAggregate): SensorAggregate = nothing
  }

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    val pravegaConfig = PravegaConfig
      .fromParams(params)
      .withDefaultScope("examples")

    // ensure that the scope and stream exist
    val stream = Utils.createStream(
      pravegaConfig,
      params.get("input", "turbineHeatTest"),
      StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build())

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // required since on a multi core CPU machine, the watermark is not advancing due to idle sources and causing window not to trigger

    // 1. read and decode the sensor events from a Pravega stream
    val source = FlinkPravegaReader.builder()
      .withPravegaConfig(pravegaConfig)
      .forStream(stream)
      .withDeserializationSchema(new SimpleStringSchema())
      .build()

    val events: DataStream[SensorEvent] = env.addSource(source).name("input").map { line =>
      val l = line.trim.split(", ")
      SensorEvent(l(0).toLong, l(1).toInt, l(2), l(3).toFloat)
    }.name("events")

    // 2. extract timestamp information to support 'event-time' processing
    val timestamped = events.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[SensorEvent](Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorEvent] {
        override def extractTimestamp(element: SensorEvent, recordTimestamp: Long): Long = element.timestamp
      }))

    // 3. summarize the temperature data for each sensor
    val summaries = timestamped
      .keyBy(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+8)))
      .aggregate(SensorAggregate)
      .name("summaries")

    // 4. save to HDFS and print to stdout.  Refer to the TaskManager's 'Stdout' view in the Flink UI.
    summaries.print().name("stdout")
    if (params.has("output")) {
      summaries.writeAsCsv(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE)
    }

    env.execute(s"TurbineHeatProcessor_" + stream)
  }
}
