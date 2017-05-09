/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection;

import io.pravega.anomalydetection.event.pipeline.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {

	private static final Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);

	public static void main(String[] args) {
		LOG.info("Starting Application Main...");
		PipelineRunner runner = new PipelineRunner();
		runner.run(args);
		LOG.info("Ending Application Main...");
		System.exit(0);
	}
}