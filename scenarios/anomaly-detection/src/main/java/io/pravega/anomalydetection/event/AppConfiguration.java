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
package io.pravega.anomalydetection.event;

import java.io.Serializable;

public class AppConfiguration implements Serializable {
	private String name;
	private Producer producer;
	private Pipeline pipeline;

	public static class Producer {
		private long latencyInMilliSec;
		private int capacity;
		private double errorProbFactor;
		private boolean controlledEnv;

		public long getLatencyInMilliSec() {
			return latencyInMilliSec;
		}

		public void setLatencyInMilliSec(long latencyInMilliSec) {
			this.latencyInMilliSec = latencyInMilliSec;
		}

		public int getCapacity() {
			return capacity;
		}

		public void setCapacity(int capacity) {
			this.capacity = capacity;
		}

		public double getErrorProbFactor() {
			return errorProbFactor;
		}

		public void setErrorProbFactor(double errorProbFactor) {
			this.errorProbFactor = errorProbFactor;
		}

		public boolean isControlledEnv() {
			return controlledEnv;
		}

		public void setControlledEnv(boolean controlledEnv) {
			this.controlledEnv = controlledEnv;
		}
	}

	public static class Pipeline {
		private int parallelism;
		private long checkpointIntervalInMilliSec;
		private int watermarkOffsetInSec;
		private int windowIntervalInSeconds;
		private boolean disableCheckpoint;
		private ElasticSearch elasticSearch;

		public int getParallelism() {
			return parallelism;
		}

		public void setParallelism(int parallelism) {
			this.parallelism = parallelism;
		}

		public long getCheckpointIntervalInMilliSec() {
			return checkpointIntervalInMilliSec;
		}

		public void setCheckpointIntervalInMilliSec(long checkpointIntervalInMilliSec) {
			this.checkpointIntervalInMilliSec = checkpointIntervalInMilliSec;
		}

		public ElasticSearch getElasticSearch() {
			return elasticSearch;
		}

		public void setElasticSearch(ElasticSearch elasticSearch) {
			this.elasticSearch = elasticSearch;
		}

		public boolean isDisableCheckpoint() {
			return disableCheckpoint;
		}

		public void setDisableCheckpoint(boolean disableCheckpoint) {
			this.disableCheckpoint = disableCheckpoint;
		}

		public int getWatermarkOffsetInSec() {
			return watermarkOffsetInSec;
		}

		public void setWatermarkOffsetInSec(int watermarkOffsetInSec) {
			this.watermarkOffsetInSec = watermarkOffsetInSec;
		}

		public int getWindowIntervalInSeconds() {
			return windowIntervalInSeconds;
		}

		public void setWindowIntervalInSeconds(int windowIntervalInSeconds) {
			this.windowIntervalInSeconds = windowIntervalInSeconds;
		}
	}

	public static class ElasticSearch {
		private boolean sinkResults;
		private String host;
		private int port;
		private String cluster;
		private String index;
		private String type;

		public boolean isSinkResults() {
			return sinkResults;
		}

		public void setSinkResults(boolean sinkResults) {
			this.sinkResults = sinkResults;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public String getCluster() {
			return cluster;
		}

		public void setCluster(String cluster) {
			this.cluster = cluster;
		}

		public String getIndex() {
			return index;
		}

		public void setIndex(String index) {
			this.index = index;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		this.producer = producer;
	}

	public Pipeline getPipeline() {
		return pipeline;
	}

	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}
}
