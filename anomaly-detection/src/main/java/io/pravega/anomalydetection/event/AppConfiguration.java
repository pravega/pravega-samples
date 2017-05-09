/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event;

import java.io.Serializable;

public class AppConfiguration implements Serializable{
	private String name;
	private Producer producer;
	private Pipeline pipeline;
	private Pravega pravega;

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
		private String stateCheckpointDir;
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

		public String getStateCheckpointDir() {
			return stateCheckpointDir;
		}

		public void setStateCheckpointDir(String stateCheckpointDir) {
			this.stateCheckpointDir = stateCheckpointDir;
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

	public static class Pravega {
		private String controllerUri;
		private String stream;
		private String scope;
		private Writer writer;

		public String getControllerUri() {
			return controllerUri;
		}

		public void setControllerUri(String controllerUri) {
			this.controllerUri = controllerUri;
		}

		public String getStream() {
			return stream;
		}

		public void setStream(String stream) {
			this.stream = stream;
		}

		public String getScope() {
			return scope;
		}

		public void setScope(String scope) {
			this.scope = scope;
		}

		public Writer getWriter() {
			return writer;
		}

		public void setWriter(Writer writer) {
			this.writer = writer;
		}
	}

	public static class Writer {
		private String routingKey;

		public String getRoutingKey() {
			return routingKey;
		}

		public void setRoutingKey(String routingKey) {
			this.routingKey = routingKey;
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

	public Pravega getPravega() {
		return pravega;
	}

	public void setPravega(Pravega pravega) {
		this.pravega = pravega;
	}
}
