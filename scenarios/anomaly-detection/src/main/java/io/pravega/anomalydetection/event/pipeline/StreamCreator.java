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
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCreator extends AbstractPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StreamCreator.class);

	public StreamCreator(AppConfiguration appConfiguration, FlinkPravegaParams pravega) {
		super(appConfiguration, pravega);
	}

	public void run() {
		StreamId streamId = getStreamId();
		pravega.createStream(streamId);
		LOG.info("Succesfully created stream: {}", streamId);
	}
}
