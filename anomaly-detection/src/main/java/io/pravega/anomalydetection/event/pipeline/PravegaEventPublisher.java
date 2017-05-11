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
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.anomalydetection.event.producer.ControlledSourceContextProducer;
import io.pravega.anomalydetection.event.producer.SourceContextProducer;
import io.pravega.anomalydetection.event.serialization.PravegaSerializationSchema;
import io.pravega.anomalydetection.event.state.Event;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaEventPublisher implements IPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(PravegaEventPublisher.class);

	@Override
	public void run(AppConfiguration appConfiguration) throws Exception {
		publishUsingFlinkConnector(appConfiguration);
	}

	private void publishUsingFlinkConnector(AppConfiguration appConfiguration) throws Exception {

		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();
		PravegaSerializationSchema<Event> pravegaSerializationSchema = new PravegaSerializationSchema<>(new JavaSerializer<Event>());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FlinkPravegaWriter<Event> writer = new FlinkPravegaWriter<>(URI.create(controllerUri),
				scope,
				stream,
				pravegaSerializationSchema,
				new EventRouter());

		int parallelism = appConfiguration.getPipeline().getParallelism();

		if(appConfiguration.getProducer().isControlledEnv()) {
			//setting this to single instance since the controlled run allows user inout to trigger error events
			env.setParallelism(1);
			long latency = appConfiguration.getProducer().getLatencyInMilliSec();
			int capacity = appConfiguration.getProducer().getCapacity();
			ControlledSourceContextProducer controlledSourceContextProducer = new ControlledSourceContextProducer(capacity, latency);
			env.addSource(controlledSourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + stream);
		} else {
			env.setParallelism(parallelism);
			SourceContextProducer sourceContextProducer = new SourceContextProducer(appConfiguration);
			env.addSource(sourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + stream);
		}

		env.execute(appConfiguration.getName()+"-producer");

	}

	public static class EventRouter implements PravegaEventRouter<Event> {
		@Override
		public String getRoutingKey(Event event) {
			return event.getNetworkId();
		}
	}
}
