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

import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.EventsGenerator;
import java.util.Optional;
import java.util.Scanner;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interactive event producer that produces valid and invalid events based on user input.
 */
public class ControlledSourceContextProducer extends RichParallelSourceFunction<Event> {

	private static final Logger log = LoggerFactory.getLogger(ControlledSourceContextProducer.class);

	private boolean running = true;
	private final int capacity;
	private boolean injectErrorRecord = false;
	private int count = 0;
	private int invalidCount = 0;
	private long latency;

	public ControlledSourceContextProducer(int capacity, long latency) {
		this.capacity = capacity;
		this.latency = latency;
	}

	@Override
	public void run(SourceContext<Event> ctx) throws Exception {
		Thread t = new Thread(new UserInputListener(this));
		t.start();
		EventsGenerator generator = new EventsGenerator(capacity, 0);
		while(running) {
			int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
			int min = range * getRuntimeContext().getIndexOfThisSubtask();
			int max = min + range;

			if(injectErrorRecord) {
				Optional<Event> event = generator.nextInvalid();
				if(event.isPresent()) {
					ctx.collect(event.get());
					invalidCount += 1;
					System.out.println(String.format(
							"*** Emitting invalid event: [%s], total count so far: [%d] ", event.get(), invalidCount));
				}
				injectErrorRecord = false;
			} else {
				Event event = generator.next(min, max);
				if(event != null) {
					ctx.collect(event);
					count += 1;
					System.out.println(String.format("Emitting event: [%s], total count so far: [%d] ", event, count));
				}
			}
			Thread.sleep(latency);
		}
		t.join();
		log.info("Exiting EventPublisher thread...");
	}

	@Override
	public void cancel() { running = false; }

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public boolean isInjectErrorRecord() {
		return injectErrorRecord;
	}

	public void setInjectErrorRecord(boolean injectErrorRecord) {
		this.injectErrorRecord = injectErrorRecord;
	}

	public static class UserInputListener implements Runnable {

		private ControlledSourceContextProducer publisher;
		public UserInputListener(ControlledSourceContextProducer publisher) {
			this.publisher = publisher;
		}

		@Override
		public void run() {
			Scanner sc = new Scanner(System.in);
			sc.useDelimiter("");

			System.out.println("#############################################################################");
			System.out.println("Usage: Press ENTER to publish an invalid event, or CTRL-C to exit.");
			System.out.println("#############################################################################");

			while(true) {
				sc.next();
				log.info("scheduled an invalid event");
				publisher.setInjectErrorRecord(true);
			}
		}
	}
}
