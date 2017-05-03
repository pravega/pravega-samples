/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.StreamManager;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class StreamHelper {

	private static final Logger LOG = LoggerFactory.getLogger(StreamHelper.class);

	public static void createStream(AppConfiguration appConfiguration) {

		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();

		StreamManager streamManager = StreamManager.create(URI.create(controllerUri));
		streamManager.createScope(scope);
		StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
		streamManager.createStream(scope, stream, streamConfig);

		LOG.info("Succesfully created stream: {} with scope: {}", stream, scope);

	}

}
