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
import io.pravega.anomalydetection.event.serialization.PravegaDeserializationSchema;
import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.Result;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.shaded.com.google.gson.Gson;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.*;

public class PravegaAnomalyDetectionProcessor implements IPipeline {

	@Override
	public void run(AppConfiguration appConfiguration) throws Exception {

		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();
		long startTime = 0;
		PravegaDeserializationSchema<Event> pravegaDeserializationSchema = new PravegaDeserializationSchema<>(Event.class, new JavaSerializer<Event>());

		final String readerName = stream + "-reader";
		FlinkPravegaReader<Event> flinkPravegaReader = new FlinkPravegaReader<>(URI.create(controllerUri), scope,
				Sets.newHashSet(stream),
				startTime,
				pravegaDeserializationSchema,
				readerName);

		int parallelism = appConfiguration.getPipeline().getParallelism();

		long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		if(!appConfiguration.getPipeline().isDisableCheckpoint()) {
			env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
		}

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Event> source = env.addSource(flinkPravegaReader).name("AnomalyEventReader");

		DataStream<Event.Alert> detector = source.keyBy("networkId")
				.flatMap(new EventStateMachineMapper())
				.name("AnomalyDetector");

		detector.print().name("Anomalies");

		long watermarkOffset = appConfiguration.getPipeline().getWatermarkOffsetInSec();
		DataStream<Event.Alert> timeExtractor = detector.assignTimestampsAndWatermarks(new EventTimeExtractor(Time.seconds(watermarkOffset)))
				.name("TimeExtractor");

		long windowIntervalInSeconds = appConfiguration.getPipeline().getWindowIntervalInSeconds();
		DataStream<Result> aggregate = timeExtractor.keyBy("networkId")
				.window(TumblingEventTimeWindows.of(Time.seconds(windowIntervalInSeconds)))
				.fold(new Result(), new FoldAlertsToResult())
				.name("Aggregate");
		aggregate.print().name("AggregatedResults");

		if(appConfiguration.getPipeline().getElasticSearch().isSinkResults()) {
			ElasticsearchSink<Result> elasticSink = sinkToElasticSearch(appConfiguration);
			aggregate.addSink(elasticSink).name("ElasticSearchSink");
		}

		env.execute(appConfiguration.getName());
	}

	public static class EventTimeExtractor extends BoundedOutOfOrdernessTimestampExtractor<Event.Alert> {

		public EventTimeExtractor(Time time) { super(time); }

		@Override
		public long extractTimestamp(Event.Alert element) {
			long timestamp = element.getEvent().getEventTime().toEpochMilli();
			return timestamp;
		}
	}

	public static class FoldAlertsToResult implements FoldFunction<Event.Alert, Result> {

		@Override
		public Result fold(Result accumulator, Event.Alert value) throws Exception {
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
	}

	private ElasticsearchSink sinkToElasticSearch(AppConfiguration appConfiguration) throws Exception {

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


		return new ElasticsearchSink (config, transports, new ResultSinkFunction(index, type));
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
					.source(resultAsJson);
		}
	}
}