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
import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.Result;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.shaded.com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processes network events to detect anomalous sequences.
 */
public class PravegaAnomalyDetectionProcessor extends AbstractPipeline {

	public PravegaAnomalyDetectionProcessor(AppConfiguration appConfiguration, PravegaConfig pravegaConfig, Stream stream) {
		super(appConfiguration, pravegaConfig, stream);
	}

	@Override
	public void run() throws Exception {

		// Configure the Pravega event reader
		FlinkPravegaReader<Event> flinkPravegaReader = FlinkPravegaReader.<Event>builder()
				.withPravegaConfig(getPravegaConfig())
				.forStream(getStreamId())
				.withDeserializationSchema(new PravegaDeserializationSchema<>(Event.class, new JavaSerializer<>()))
				.build();

		// Configure the Flink job environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(appConfiguration.getPipeline().getParallelism());
		if(!appConfiguration.getPipeline().isDisableCheckpoint()) {
			long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();
			env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
		}

		// Construct a dataflow program:

		// 1. read network events
		DataStream<Event> events = env.addSource(flinkPravegaReader).name("AnomalyEventReader");
		events.print();

		// 2. detect anomalies in event sequences
		DataStream<Event.Alert> anomalies = events
				.keyBy(Event::getSourceAddress)
				.flatMap(new EventStateMachineMapper())
				.name("AnomalyDetector");
		anomalies.print();

		// 3. aggregate the alerts by network over time
		long maxOutOfOrderness = appConfiguration.getPipeline().getWatermarkOffsetInSec();
		DataStream<Event.Alert> timestampedAnomalies = anomalies
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<Event.Alert>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderness))
						.withTimestampAssigner((event, timestamp) -> event.getEvent().getEventTime().toEpochMilli()))
				.name("TimeExtractor");

		long windowIntervalInSeconds = appConfiguration.getPipeline().getWindowIntervalInSeconds();
		DataStream<Result> aggregate = timestampedAnomalies
				.keyBy(Event.Alert::getNetworkId)
				.window(TumblingEventTimeWindows.of(Time.seconds(windowIntervalInSeconds)))
				.aggregate(new AggregateAlertsToResult())
				.name("Aggregate");
		aggregate.print();

		// 4. emit the alerts to Elasticsearch (optional)
		if(appConfiguration.getPipeline().getElasticSearch().isSinkResults()) {
			ElasticsearchSink<Result> elasticSink = sinkToElasticSearch(appConfiguration);
			aggregate.addSink(elasticSink).name("ElasticSearchSink");
		}

		// Execute the program in the Flink environment
		env.execute(appConfiguration.getName());
	}

	public static class AggregateAlertsToResult implements AggregateFunction<Event.Alert, Result, Result> {

		@Override
		public Result createAccumulator() {
			return new Result();
		}

		@Override
		public Result add(Event.Alert value, Result accumulator) {
			accumulator.setCount(accumulator.getCount() + 1);
			accumulator.setNetworkId(value.getNetworkId());
			accumulator.getIpAddress().add(Event.EventType.formatAddress(value.getEvent().getSourceAddress()));
			if(accumulator.getMinTimestamp() == 0) {
				accumulator.setMinTimestamp(value.getEvent().getEventTime().toEpochMilli());
				accumulator.setMaxTimestamp(value.getEvent().getEventTime().toEpochMilli());
			} else {
				Instant d1 = Instant.ofEpochMilli(accumulator.getMinTimestamp());
				Instant d2 = value.getEvent().getEventTime();
				if(d1.isBefore(d2)) {
					accumulator.setMinTimestamp(d1.toEpochMilli());
					accumulator.setMaxTimestamp(d2.toEpochMilli());
				} else {
					accumulator.setMinTimestamp(d2.toEpochMilli());
					accumulator.setMaxTimestamp(d1.toEpochMilli());
				}
			}
			if(accumulator.getLocation() == null) {
				accumulator.setLocation(value.getEvent().getLatlon().getLat() + "," + value.getEvent().getLatlon().getLon());
			}
			return accumulator;
		}

		@Override
		public Result getResult(Result accumulator) {
			return accumulator;
		}

		@Override
		public Result merge(Result a, Result b) {
			// This will not be called in time window
			return null;
		}
	}

	private ElasticsearchSink<Result> sinkToElasticSearch(AppConfiguration appConfiguration) throws Exception {

		String host = appConfiguration.getPipeline().getElasticSearch().getHost();
		int port = appConfiguration.getPipeline().getElasticSearch().getPort();
		String cluster = appConfiguration.getPipeline().getElasticSearch().getCluster();

		String index = appConfiguration.getPipeline().getElasticSearch().getIndex();
		String type = appConfiguration.getPipeline().getElasticSearch().getType();

		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", cluster);
		config.put("client.transport.sniff", "false");

		final InetSocketAddress socketAddress = new InetSocketAddress(host,port);

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(socketAddress);


		return new ElasticsearchSink<>(config, transports, new ResultSinkFunction(index, type));
	}

	public static class ResultSinkFunction implements ElasticsearchSinkFunction<Result> {

		private final String index;
		private final String type;

		public ResultSinkFunction(String index, String type) {
			this.index = index;
			this.type = type;
		}

		@Override
		public void process(Result element, RuntimeContext ctx, RequestIndexer indexer) {
			indexer.add(createIndexRequest(element));
		}

		private IndexRequest createIndexRequest(Result element) {
			Gson gson = new Gson();
			String resultAsJson = gson.toJson(element);
			return Requests.indexRequest()
					.index(index)
					.type(type)
					.id(element.getNetworkId())
					.source(resultAsJson, XContentFactory.xContentType(resultAsJson));
		}
	}
}