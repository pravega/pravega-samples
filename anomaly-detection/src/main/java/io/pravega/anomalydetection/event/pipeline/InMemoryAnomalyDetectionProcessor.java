/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.anomalydetection.event.producer.SourceContextProducer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryAnomalyDetectionProcessor implements IPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryAnomalyDetectionProcessor.class);

	@Override
	public void run(AppConfiguration appConfiguration) throws Exception {

		int parallelism = appConfiguration.getPipeline().getParallelism();

		long checkpointInterval = appConfiguration.getPipeline().getCheckpointIntervalInMilliSec();

		SourceContextProducer eventsGeneratorSource = new SourceContextProducer(appConfiguration);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointInterval);

		env.addSource(eventsGeneratorSource)
				.keyBy("networkId")
				.flatMap(new EventStateMachineMapper())
				.print();

		env.execute();

	}
}
