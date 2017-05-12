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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class StreamHelper {

	private static final Logger LOG = LoggerFactory.getLogger(StreamHelper.class);

	public static void createStream(AppConfiguration appConfiguration) {

		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();
		int segmentCount = appConfiguration.getPipeline().getParallelism();

		StreamManager streamManager = StreamManager.create(URI.create(controllerUri));
		streamManager.createScope(scope);
		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(segmentCount))
				.build();
		streamManager.createStream(scope, stream, streamConfig);

		LOG.info("Succesfully created stream: {} with scope: {}", stream, scope);

	}

}
