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

public abstract class AbstractPipeline {
	public static final String STREAM_PARAMETER = "stream";
	public static final String DEFAULT_STREAM = "examples/NetworkPacket";

	protected AppConfiguration appConfiguration;
	protected FlinkPravegaParams pravega;

	public AbstractPipeline(AppConfiguration appConfiguration, FlinkPravegaParams pravega) {
		this.appConfiguration = appConfiguration;
		this.pravega = pravega;
	}

	public StreamId getStreamId() {
		return pravega.getStreamFromParam(STREAM_PARAMETER, DEFAULT_STREAM);
	}

	public abstract void run() throws Exception;
}
