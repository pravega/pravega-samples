/**
  *
  * Copyright (c) 2017 Dell Inc., or its subsidiaries.
  *
  */
package com.emc.pravega.examples.flink.util

import com.emc.pravega.stream.StreamConfiguration
import com.emc.pravega.stream.impl.Controller

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Helper methods for streams.
  */
object Streams {

  /**
    * Create a scope.
    *
    * The operation is successful even if the stream already exists.
    *
    * @param scope the scope name.
    * @param controller the controller to use.
    * @return the created scope name.
    */
  def createScope(scope: String)(implicit controller: Controller, ec: ExecutionContext): Future[String] = {
    controller.createScope(scope).toScala.map(_ => scope)
  }

  /**
    * Create a stream.
    *
    * The operation is successful even if the stream already exists.
    *
    * @param scope the name of an existing scope.
    * @param stream the stream name.
    * @param controller the controller to use.
    * @return the reated stream name.
    */
  def createStream(scope: String, stream: String)(implicit controller: Controller, ec: ExecutionContext): Future[String] = {
    val config = StreamConfiguration.builder()
      .scope(scope)
      .streamName(stream)
      .build()
    controller.createStream(config).toScala.map(_ => stream)
  }
}
