/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaStandardStreamWriter<T> {

	private static final Logger LOG = LoggerFactory.getLogger(PravegaStandardStreamWriter.class);

	private final URI controllerURI;

	private final String streamName;

	private final String scope;

	private final String routingKey;

	private EventStreamWriter<T> writer;

	private final Serializer<T> serializer;

	public PravegaStandardStreamWriter(final String controllerUri,
									   final String streamName,
									   final String scope,
									   final String routingKey,
									   final Serializer<T> serializer ) {

		Preconditions.checkNotNull(controllerUri);
		Preconditions.checkNotNull(scope);
		Preconditions.checkNotNull(streamName);
		Preconditions.checkNotNull(routingKey);
		Preconditions.checkNotNull(serializer);

		this.controllerURI = URI.create(controllerUri);
		this.streamName = streamName;
		this.scope = scope;
		this.routingKey = routingKey;
		this.serializer = serializer;

		initialize();
	}

	private void initialize() {
		StreamManager streamManager = StreamManager.create(controllerURI);

		//create scope
		streamManager.createScope(scope);

		//define scaling policy
		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(1))
				.build();

		//create stream
		streamManager.createStream(scope, streamName, streamConfig);

		//write to the stream
		ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);

		writer = clientFactory.createEventWriter(streamName,serializer, EventWriterConfig.builder().build());

	}

	public void closeStream() {
		if(writer != null) {
			writer.close();
		}
	}

	public void writeToStream(T event) {
		LOG.info("Writing [{}] to the [{}] with the scope [{}] using routing key [{}]",	event, streamName, scope, routingKey);
		writer.writeEvent(routingKey, event);
	}
}
