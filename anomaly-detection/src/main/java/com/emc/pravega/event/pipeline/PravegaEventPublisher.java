/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.event.pipeline;

import com.emc.pravega.StreamManager;
import com.emc.pravega.connectors.flink.FlinkPravegaWriter;
import com.emc.pravega.connectors.flink.PravegaEventRouter;
import com.emc.pravega.connectors.flink.PravegaWriterMode;
import com.emc.pravega.event.AppConfiguration;
import com.emc.pravega.event.producer.SourceContextProducer;
import com.emc.pravega.event.serialization.PravegaSerializationSchema;
import com.emc.pravega.event.state.Event;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaEventPublisher implements IPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(PravegaEventPublisher.class);

	@Override
	public void run(AppConfiguration appConfiguration) throws Exception {
		//publishUsingStandardWriter(appConfiguration);
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

		SourceContextProducer eventsGeneratorSource = new SourceContextProducer(appConfiguration);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		env.addSource(eventsGeneratorSource).addSink(writer);

		env.execute();

	}

	private void publishUsingStandardWriter(AppConfiguration appConfiguration) throws Exception {
		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();
		String routingKey = appConfiguration.getPravega().getWriter().getRoutingKey();
		JavaSerializer<Event> serializer = new JavaSerializer<>();
		PravegaStandardStreamWriter<Event> writer = new PravegaStandardStreamWriter<>(controllerUri, stream, scope, routingKey, serializer);

		SourceContextProducer eventsGeneratorSource = new SourceContextProducer(appConfiguration);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.addSource(eventsGeneratorSource).addSink((Event event) -> {
			writer.writeToStream(event);
		});
		env.execute("Pravega Event Publisher");

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
