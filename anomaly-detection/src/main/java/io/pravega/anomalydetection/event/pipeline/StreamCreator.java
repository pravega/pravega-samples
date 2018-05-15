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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCreator extends AbstractPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StreamCreator.class);

	public StreamCreator(AppConfiguration appConfiguration, PravegaConfig pravegaConfig, Stream stream) {
		super(appConfiguration, pravegaConfig, stream);
	}

	public void run() {
		Stream streamId = getStreamId();

		try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
			// create the requested scope (if necessary)
			streamManager.createScope(stream.getScope());

			// create the requested stream
			StreamConfiguration streamConfig = StreamConfiguration.builder().build();
			streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
		}

		LOG.info("Successfully created stream: {}", streamId);
	}
}
