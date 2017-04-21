/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.event.pipeline;

import com.emc.pravega.StreamManager;
import com.emc.pravega.event.AppConfiguration;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
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
