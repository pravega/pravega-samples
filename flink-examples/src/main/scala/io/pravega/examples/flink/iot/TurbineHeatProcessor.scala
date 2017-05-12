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
package io.pravega.examples.flink.iot

import java.net.URI

import io.pravega.client.admin.StreamManager
import io.pravega.connectors.flink.FlinkPravegaReader
import io.pravega.examples.flink.util.PravegaParameters
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.client.stream.impl.JavaSerializer
import io.pravega.examples.flink.util.serialization.PravegaDeserializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConversions._
import scala.math._

/**
  * Sample Flink Streaming program to process temperature data produced by the
  * accompanying TurbineHeatSensor app.
  */
object TurbineHeatProcessor {

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

  object SensorAggregate {
    val nothing = null.asInstanceOf[SensorAggregate]

    /**
      * Update the aggregate record as new events arrive.
      */
    def fold(acc: SensorAggregate, evt: SensorEvent) = {
      acc match {
        case null =>
          SensorAggregate(evt.timestamp, evt.sensorId, evt.location, (evt.temp, evt.temp))
        case _ =>
          SensorAggregate(acc.startTime, evt.sensorId, evt.location, (min(evt.temp, acc.tempRange._1), max(acc.tempRange._2, evt.temp)))
      }
    }
  }

  def main(args: Array[String]) {
    val params = Parameters(ParameterTool.fromArgs(args))

    // ensure that the scope and stream exist
    ensureStream(params)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. read and decode the sensor events from a Pravega stream
    val events: DataStream[SensorEvent] = env.addSource(new FlinkPravegaReader(
      params.controllerUri,
      params.scope,
      Set(params.stream),
      params.startTime,
      new PravegaDeserializationSchema(classOf[String], new JavaSerializer[String]()))).map { line =>
      val l = line.trim.split(", ")
      SensorEvent(l(0).toLong, l(1).toInt, l(2), l(3).toFloat)
    }.name("events")

    // 2. extract timestamp information to support 'event-time' processing
    val timestamped = events.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorEvent](Time.seconds(10)) {
        override def extractTimestamp(element: SensorEvent): Long = element.timestamp
      })

    // 3. summarize the temperature data for each sensor
    val summaries = timestamped
      .keyBy(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+8)))
      .fold(SensorAggregate.nothing)(SensorAggregate.fold)
      .name("summaries")

    // 4. save to HDFS and print to stdout.  Refer to the TaskManager's 'Stdout' view in the Flink UI.
    summaries.print().name("stdout")
    for(path <- params.outputPath) {
      summaries.writeAsCsv(path, FileSystem.WriteMode.OVERWRITE)
    }

    env.execute(s"Turbine Heat Processor (${params.scope}, ${params.stream})")
  }

  /**
    * Ensure that the configured Pravega stream exists.
    */
  private def ensureStream(params: Parameters) = {
    val streamManager = StreamManager.create(params.controllerUri)

    // create the scope (if necessary)
    streamManager.createScope(params.scope)

    // create the stream (if necessary)
    val streamConfig = StreamConfiguration.builder
      .scope(params.scope).streamName(params.stream).scalingPolicy(ScalingPolicy.fixed(1)).build
    streamManager.createStream(params.scope, params.stream, streamConfig)
  }

  /**
    * Configuration parameters for the Flink program.
    */
  case class Parameters(p: ParameterTool) {
    val DEFAULT_SCOPE_NAME = "turbine"
    val DEFAULT_STREAM_NAME = "turbineHeatTest"
    val DEFAULT_START_TIME = 0

    lazy val controllerUri = new URI(p.get("controller", PravegaParameters.DEFAULT_CONTROLLER_URI))
    lazy val scope = p.get("scope", DEFAULT_SCOPE_NAME)
    lazy val stream = p.get("stream", DEFAULT_STREAM_NAME)
    lazy val startTime = p.getLong("start", DEFAULT_START_TIME)
    lazy val outputPath = Option(p.get("output"))
  }
}
