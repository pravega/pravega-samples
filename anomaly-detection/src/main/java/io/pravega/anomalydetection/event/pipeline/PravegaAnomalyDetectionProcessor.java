/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
import org.apache.flink.shaded.org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.net.InetSocketAddress;
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
		if(!appConfiguration.getPipeline().isDisableCheckpoint()) {
			env.enableCheckpointing(checkpointInterval);
		}
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Event> source = env.addSource(flinkPravegaReader).name("Event Reader");

		long watermarkOffset = appConfiguration.getPipeline().getWatermarkOffsetInSec();
		DataStream<Event> timeExtractor = source.assignTimestampsAndWatermarks(new EventTimeExtractor(Time.seconds(watermarkOffset)))
				.name("Time Extractor");

		DataStream<Event.Alert> detector = timeExtractor.keyBy("networkId")
				.flatMap(new EventStateMachineMapper())
				.name("Anomaly Detector");

		detector.print().name("anomalies");

		long windowIntervalInSeconds = appConfiguration.getPipeline().getWindowIntervalInSeconds();
		DataStream<Result> aggregate = detector.keyBy("networkId")
				.window(TumblingEventTimeWindows.of(Time.seconds(windowIntervalInSeconds)))
				.fold(new Result(), new FoldAlertsToResult())
				.name("Aggregate");
		aggregate.print().name("Aggregated Results");

		if(appConfiguration.getPipeline().getElasticSearch().isSinkResults()) {
			ElasticsearchSink<Result> elasticSink = sinkToElasticSearch(appConfiguration);
			aggregate.addSink(elasticSink).name("Elastic Search");
			aggregate.print().name("Final Results");
		}

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

	public static class FoldAlertsToResult implements FoldFunction<Event.Alert, Result> {

		@Override
		public Result fold(Result accumulator, Event.Alert value) throws Exception {
			accumulator.setCount(accumulator.getCount() + 1);
			accumulator.setNetworkId(value.getNetworkId());
			accumulator.getIpAddress().add(Event.EventType.formatAddress(value.getEvent().getSourceAddress()));
			if(accumulator.getMinTimestamp() == 0) {
				accumulator.setMinTimestamp(Date.from(value.getEvent().getEventTime()).getTime());
				accumulator.setMaxTimestamp(Date.from(value.getEvent().getEventTime()).getTime());
			} else {
				long ts = Date.from( value.getEvent().getEventTime()).getTime();
				Date d1 = new Date(accumulator.getMinTimestamp());
				Date d2 = new Date(ts);
				if(d1.before(d2)) {
					accumulator.setMinTimestamp(d1.getTime());
					accumulator.setMaxTimestamp(d2.getTime());
				} else {
					accumulator.setMinTimestamp(d2.getTime());
					accumulator.setMaxTimestamp(d1.getTime());
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