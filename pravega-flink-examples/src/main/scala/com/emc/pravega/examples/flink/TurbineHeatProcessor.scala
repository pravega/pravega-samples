/**
  *
  * Copyright (c) 2017 Dell Inc., or its subsidiaries.
  *
  */
package com.emc.pravega.examples.flink

import java.net.URI

import scala.math._
import com.emc.pravega.connectors.flink.FlinkPravegaReader
import com.emc.pravega.examples.flink.Streams._
import com.emc.pravega.examples.flink.serialization.PravegaDeserializationSchema
import com.emc.pravega.stream.impl.{ControllerImpl, JavaSerializer}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Sample Flink Streaming program to process temperature data produced by the
  * accompanying TurbineHeatSensor app.
  *
  * See https://asdstash.isus.emc.com/projects/NAUT/repos/platform/browse/pravega-sanity/src/main/java/com/emc/nautilus/demo/TurbineHeatSensor.java
  */
object TurbineHeatProcessor {

  case class SensorEvent(timestamp: Long, sensorId: Int, location: String, temp: Float)

  case class SensorAggregate(sensorId: Int, location: String, tempRange: (Float,Float))

  object SensorAggregate {
    val nothing = null.asInstanceOf[SensorAggregate]
    def fold(acc: SensorAggregate, evt: SensorEvent): SensorAggregate = {
      acc match {
        case null =>
          SensorAggregate(evt.sensorId, evt.location, (evt.temp, evt.temp))
        case _ =>
          SensorAggregate(evt.sensorId, evt.location, (min(evt.temp, acc.tempRange._1), max(acc.tempRange._2, evt.temp)))
      }
    }
  }

  def main(args: Array[String]) {
    val params = Parameters(ParameterTool.fromArgs(args))
    try {
      run(params)
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def run(params: Parameters) {

    // ensure that the scope and stream exist
    ensureStream(params)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // construct a dataflow graph

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
      .keyBy("sensorId")
      .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+8)))
      .fold(SensorAggregate.nothing)(SensorAggregate.fold)
      .name("summaries")

    // 4. print to stdout.  Refer to the TaskManager's 'Stdout' view in the Flink UI.
    summaries.print().name("stdout")

    env.execute(s"Turbine Heat Processor (${params.scope}, ${params.stream})")
  }

  private def ensureStream(params: Parameters) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val controller = new ControllerImpl(params.controllerUri.getHost, params.controllerUri.getPort)
    Await.result(createScope(params.scope).flatMap(createStream(_, params.stream)), 1 minute)
  }

  case class Parameters(p: ParameterTool) {
    val DEFAULT_SCOPE_NAME = "turbine"
    val DEFAULT_STREAM_NAME = "turbineHeatTest"
    val DEFAULT_START_TIME = 0

    lazy val controllerUri = new URI(p.get("controller", PravegaParameters.DEFAULT_CONTROLLER_URI))
    lazy val scope = p.get("scope", DEFAULT_SCOPE_NAME)
    lazy val stream = p.get("stream", DEFAULT_STREAM_NAME)
    lazy val startTime = p.getLong("start", DEFAULT_START_TIME)
  }
}
