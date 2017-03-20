/**
  *
  * Copyright (c) 2017 Dell Inc., or its subsidiaries.
  *
  */
package com.emc.pravega.examples.flink

import java.net.URI

import com.emc.pravega.connectors.flink.FlinkPravegaReader
import com.emc.pravega.examples.flink.Streams._
import com.emc.pravega.examples.flink.serialization.PravegaDeserializationSchema
import com.emc.pravega.stream.impl.{ControllerImpl, JavaSerializer}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

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

    // construct a dataflow graph
    val events: DataStream[String] = env.addSource(new FlinkPravegaReader(
      params.controllerUri,
      params.scope,
      Set(params.stream),
      params.startTime,
      new PravegaDeserializationSchema(classOf[String], new JavaSerializer[String]())))

    events.print()

    env.execute("Turbine Heat Processor")
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
