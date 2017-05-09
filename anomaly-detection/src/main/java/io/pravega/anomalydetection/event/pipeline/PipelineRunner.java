/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.shaded.com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class PipelineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

	private static final String configFile = "app.json";

	private AppConfiguration appConfiguration;

	private int runMode;

	private void parseConfigurations(String[] args) {

		LOG.info("ApplicationMain Main.. Arguments: {}", Arrays.asList(args));

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		LOG.info("Parameter Tool: {}", parameterTool.toMap());

		if(parameterTool.getNumberOfParameters() != 2) {
			printUsage();
			System.exit(1);
		}

		String configDirPath = parameterTool.getRequired("configDir");
		try {
			byte[] configurationData = Files.readAllBytes(Paths.get(configDirPath + File.separator + configFile));
			String jsonData = new String(configurationData);
			LOG.info("App Configurations raw data: {}", jsonData);
			Gson gson = new Gson();
			appConfiguration = gson.fromJson(jsonData, AppConfiguration.class);
		} catch (IOException e) {
			LOG.error("Could not read {}",configFile, e);
			System.exit(1);
		}

		runMode = parameterTool.getInt("mode");

	}

	private void printUsage() {
		StringBuilder message = new StringBuilder();
		message.append("\n############################################################################################################\n");
		message.append("Usage: com.emc.pravega.ApplicationMain --configDir <app.json file location> --mode <1 or 2 or 3>").append("\n");
		message.append("Mode 1 == Create pravega stream as defined in the configuration file").append("\n");
		message.append("Mode 2 == Publish streaming events to Pravega").append("\n");
		message.append("Mode 3 == Run Anomaly Detection by reading from Pravega stream").append("\n");
		message.append("############################################################################################################");
		LOG.error("{}", message.toString());
	}


	public void run(String[] args) {

		parseConfigurations(args);

		try {

			IPipeline pipeline;
			switch (runMode) {
				case 1:
					LOG.info("Going to create Pravega stream");
					StreamHelper.createStream(appConfiguration);
					break;
				case 2:
					LOG.info("Running event publisher to publish events to Pravega stream");
					pipeline = new PravegaEventPublisher();
					pipeline.run(appConfiguration);
					break;
				case 3:
					LOG.info("Running anomaly detection by reading from Pravega stream");
					pipeline = new PravegaAnomalyDetectionProcessor();
					pipeline.run(appConfiguration);
					break;
				default:
					LOG.error("Incorrect run mode [{}] specified", runMode);
					printUsage();
			}
		} catch (Exception e) {
			LOG.error("Failed to run the pipeline.", e);
		}

	}
}
