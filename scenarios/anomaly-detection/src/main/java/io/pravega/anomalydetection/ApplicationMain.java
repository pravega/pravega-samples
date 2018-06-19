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
package io.pravega.anomalydetection;

import io.pravega.anomalydetection.event.pipeline.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {

	private static final Logger log = LoggerFactory.getLogger(ApplicationMain.class);

	public static void main(String[] args) {
		log.info("Starting Application Main...");
		PipelineRunner runner = new PipelineRunner();
		runner.run(args);
		log.info("Ending Application Main...");
		System.exit(0);
	}
}