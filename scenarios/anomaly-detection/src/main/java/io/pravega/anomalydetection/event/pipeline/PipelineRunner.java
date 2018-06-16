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
import io.pravega.shaded.com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public class PipelineRunner {

	public static final String STREAM_PARAMETER = "stream";
	public static final String DEFAULT_SCOPE = "examples";
	public static final String DEFAULT_STREAM = "NetworkPacket";

	private static final String configFile = "app.json";

	private AppConfiguration appConfiguration;
	private PravegaConfig pravegaConfig;
	private int runMode;
	private Stream stream;

	private void parseConfigurations(String[] args) {

		log.info("ApplicationMain Main.. Arguments: {}", Arrays.asList(args));

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		log.info("Parameter Tool: {}", parameterTool.toMap());

		if(!parameterTool.has("mode")) {
			printUsage();
			System.exit(1);
		}

		String configDirPath = parameterTool.get("configDir", "conf");
		try {
			byte[] configurationData = Files.readAllBytes(Paths.get(configDirPath + File.separator + configFile));
			String jsonData = new String(configurationData);
			log.info("App Configurations raw data: {}", jsonData);
			Gson gson = new Gson();
			appConfiguration = gson.fromJson(jsonData, AppConfiguration.class);
		} catch (IOException e) {
			log.error("Could not read {}",configFile, e);
			System.exit(1);
		}

		runMode = parameterTool.getInt("mode");
		pravegaConfig = PravegaConfig.fromParams(parameterTool).withDefaultScope(DEFAULT_SCOPE);
		stream = pravegaConfig.resolve(parameterTool.get(STREAM_PARAMETER, DEFAULT_STREAM));
	}

	private void printUsage() {
		StringBuilder message = new StringBuilder();
		message.append("\n############################################################################################################\n");
		message.append("Options:").append("\n");
		message.append("  --configDir='conf': the directory containing the configuration file (app.json)").append("\n");
		message.append("  --mode: 1 or 2 or 3 (see below for details)").append("\n");
		message.append("  --controller='tcp://localhost:9090': the Pravega controller URI").append("\n");
		message.append("  --scope='examples': the Pravega scope to use").append("\n");
		message.append("  --stream='NetworkPacket': the Pravega stream to use").append("\n");
		message.append("Modes:").append("\n");
		message.append("  1: Create a Pravega stream").append("\n");
		message.append("  2: Publish streaming events to a Pravega stream").append("\n");
		message.append("  3: Run Anomaly Detection over events from a Pravega stream").append("\n");
		message.append("############################################################################################################");
		log.error("{}", message.toString());
	}


	public void run(String[] args) {

		parseConfigurations(args);

		try {
			AbstractPipeline pipeline = null;
			switch (runMode) {
				case 1:
					log.info("Going to create Pravega stream");
					pipeline = new StreamCreator(appConfiguration, pravegaConfig, stream);
					break;
				case 2:
					log.info("Running event publisher to publish events to Pravega stream");
					pipeline = new PravegaEventPublisher(appConfiguration, pravegaConfig, stream);
					break;
				case 3:
					log.info("Running anomaly detection by reading from Pravega stream");
					pipeline = new PravegaAnomalyDetectionProcessor(appConfiguration, pravegaConfig, stream);
					break;
				default:
					log.error("Incorrect run mode [{}] specified", runMode);
					printUsage();
					System.exit(-1);
			}
			pipeline.run();
		} catch (Exception e) {
			log.error("Failed to run the pipeline.", e);
		}

	}
}
