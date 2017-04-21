/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.event.pipeline;

import com.emc.pravega.StreamManager;
import com.emc.pravega.connectors.flink.FlinkPravegaReader;
import com.emc.pravega.event.AppConfiguration;
import com.emc.pravega.event.serialization.PravegaDeserializationSchema;
import com.emc.pravega.event.state.Event;
import com.emc.pravega.shaded.com.google.common.collect.Sets;
import com.emc.pravega.stream.impl.JavaSerializer;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URI;
import java.util.*;

public class PravegaAnomalyDetectionProcessor implements IPipeline {

	@Override
	public void run(AppConfiguration appConfiguration) throws Exception {

		String controllerUri = appConfiguration.getPravega().getControllerUri();
		String scope = appConfiguration.getPravega().getScope();
		String stream = appConfiguration.getPravega().getStream();
		long startTime = 0;
		PravegaDeserializationSchema<Event> pravegaDeserializationSchema = new PravegaDeserializationSchema<>(Event.class, new JavaSerializer<Event>());

		FlinkPravegaReader<Event> flinkPravegaReader = new FlinkPravegaReader<>(URI.create(controllerUri), scope,
				Sets.newHashSet(stream),
				startTime,
				pravegaDeserializationSchema);

		int parallelism = appConfiguration.getPipeline().getParallelism();
		long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointInterval);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Event> source = env.addSource(flinkPravegaReader).name("Event Reader");

		long watermarkOffset = 5;
		DataStream<Event> timeExtractor = source.assignTimestampsAndWatermarks(new EventTimeExtractor(Time.seconds(watermarkOffset)))
				.name("Time Extractor");

		DataStream<Event.Alert> detector = timeExtractor.keyBy("networkId")
				.flatMap(new EventStateMachineMapper())
				.name("Anomaly Detector");

		detector.print().name("anomalies");

		Map<String, List<Event.Alert>> resultsMap = new HashMap<>();

		long windowIntervalInSeconds = 30;
		DataStream<Map<String, List<Event.Alert>>> aggregate = detector.keyBy("networkId")
				.window(TumblingEventTimeWindows.of(Time.seconds(windowIntervalInSeconds)))
				.fold(resultsMap, new FoldEvents())
				.name("Aggregate");

		aggregate.print().name("Aggregated Results");

		env.execute(appConfiguration.getName());
	}

	public static class EventTimeExtractor extends BoundedOutOfOrdernessTimestampExtractor<Event> {

		public EventTimeExtractor(Time time) { super(time); }

		@Override
		public long extractTimestamp(Event element) {
			long timestamp = Date.from( element.getEventTime()).getTime();
			return timestamp;
		}
	}

	public static class FoldEvents implements FoldFunction<Event.Alert, Map<String, List<Event.Alert>>> {
		@Override
		public Map<String, List<Event.Alert>> fold(Map<String, List<Event.Alert>> accumulator, Event.Alert alert) throws Exception {
			if(accumulator.containsKey(alert.getNetworkId())) {
				accumulator.get(alert.getNetworkId()).add(alert);
			} else {
				List<Event.Alert> alerts = new ArrayList<>();
				alerts.add(alert);
				accumulator.put(alert.getNetworkId(),alerts);
			}
			return accumulator;
		}
	}


}
