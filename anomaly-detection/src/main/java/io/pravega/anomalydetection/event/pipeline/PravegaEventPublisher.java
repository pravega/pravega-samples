/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
import io.pravega.connectors.flink.PravegaWriterMode;
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

		String routingKey = appConfiguration.getPravega().getWriter().getRoutingKey();
		PravegaEventRouter router = new EventRouter(routingKey);

		FlinkPravegaWriter writer = new FlinkPravegaWriter(URI.create(controllerUri), scope, stream, pravegaSerializationSchema, router);
		writer.setPravegaWriterMode(PravegaWriterMode.ATLEAST_ONCE);

		int parallelism = 1;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		if(appConfiguration.getProducer().isControlledEnv()) {
			long latency = appConfiguration.getProducer().getLatencyInMilliSec();
			int capacity = appConfiguration.getProducer().getCapacity();
			ControlledSourceContextProducer controlledSourceContextProducer = new ControlledSourceContextProducer(capacity, latency);
			env.addSource(controlledSourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + stream);
		} else {
			SourceContextProducer sourceContextProducer = new SourceContextProducer(appConfiguration);
			env.addSource(sourceContextProducer).name("EventSource").addSink(writer).name("Pravega-" + stream);
		}

		env.execute(appConfiguration.getName()+"-producer");

	}

	public static class EventRouter implements PravegaEventRouter<Event> {

		private String routingKey;
		public EventRouter(String routingKey) {
			this.routingKey = routingKey;
		}

		@Override
		public String getRoutingKey(Event event) {
			return routingKey;
		}
	}
}
