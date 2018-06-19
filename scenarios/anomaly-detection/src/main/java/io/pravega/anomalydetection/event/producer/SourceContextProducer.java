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
package io.pravega.anomalydetection.event.producer;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.EventsGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceContextProducer extends RichParallelSourceFunction<Event> {

	private static final Logger log = LoggerFactory.getLogger(SourceContextProducer.class);

	private int count = 0;
	private boolean running = true;
	private final long latency;
	private final int capacity;
	private final double errorProbFactor;

	public SourceContextProducer(AppConfiguration configuration) {
		this.latency = configuration.getProducer().getLatencyInMilliSec();
		this.capacity = configuration.getProducer().getCapacity();
		this.errorProbFactor = configuration.getProducer().getErrorProbFactor();
	}

	/**
	 * @param ctx The context to emit elements to and for accessing locks.
	 */
	@Override
	public void run(SourceContext<Event> ctx) throws Exception {
		EventsGenerator generator = new EventsGenerator(capacity, errorProbFactor);
		while (running) {
			int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
			int min = range * getRuntimeContext().getIndexOfThisSubtask();
			int max = min + range;
			Event event = generator.next(min, max);
			if(event != null) {
				ctx.collect(event);
				count += 1;
				log.info("Emitting event: [{}], total count so far: [{}] ", event, count);
			}
			Thread.sleep(latency);
		}
		log.info("Exiting SourceContextProducer...");
	}

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run(SourceContext)} method. The implementation needs to ensure that the
	 * source will break out of that loop after this method is called.
	 * <p>
	 */
	@Override
	public void cancel() {
		running = false;
	}

}