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
import io.pravega.anomalydetection.event.state.Event;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PravegaEventPublisher extends AbstractPipeline {

	public PravegaEventPublisher(AppConfiguration appConfiguration, FlinkPravegaParams pravega) {
		super(appConfiguration, pravega);
	}

	@Override
	public void run() throws Exception {
		publishUsingFlinkConnector(appConfiguration);
	}

	private void publishUsingFlinkConnector(AppConfiguration appConfiguration) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamId streamId = getStreamId();
		FlinkPravegaWriter<Event> writer = pravega.newWriter(streamId, Event.class, new EventRouter());

		int parallelism = appConfiguration.getPipeline().getParallelism();

		if(appConfiguration.getProducer().isControlledEnv()) {
			if(!(env instanceof LocalStreamEnvironment)) {
				throw new Exception("Use a local Flink environment or set controlledEnv to false in app.json.");
			}
			//setting this to single instance since the controlled run allows user inout to trigger error events
			env.setParallelism(1);
			long latency = appConfiguration.getProducer().getLatencyInMilliSec();
			int capacity = appConfiguration.getProducer().getCapacity();
			ControlledSourceContextProducer controlledSourceContextProducer = new ControlledSourceContextProducer(capacity, latency);
			env.addSource(controlledSourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + streamId.getName());
		} else {
			env.setParallelism(parallelism);
			SourceContextProducer sourceContextProducer = new SourceContextProducer(appConfiguration);
			env.addSource(sourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + streamId.getName());
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
