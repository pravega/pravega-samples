/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega;

import com.emc.pravega.event.pipeline.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {

	private static final Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);

	public static void main(String[] args) {
		PipelineRunner runner = new PipelineRunner();
		runner.run(args);
	}
}