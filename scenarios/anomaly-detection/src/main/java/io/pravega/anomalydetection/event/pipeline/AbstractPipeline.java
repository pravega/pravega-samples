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
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.util.Preconditions;

public abstract class AbstractPipeline {
	protected AppConfiguration appConfiguration;
	protected PravegaConfig pravegaConfig;
	protected Stream stream;

	public AbstractPipeline(AppConfiguration appConfiguration, PravegaConfig pravegaConfig, Stream stream) {
		this.appConfiguration = Preconditions.checkNotNull(appConfiguration, "appConfiguration");
		this.pravegaConfig = Preconditions.checkNotNull(pravegaConfig, "pravegaConfig");
		this.stream = Preconditions.checkNotNull(stream, "stream");
	}

	public PravegaConfig getPravegaConfig() {
		return pravegaConfig;
	}

	public Stream getStreamId() {
		return stream;
	}

	public abstract void run() throws Exception;
}
