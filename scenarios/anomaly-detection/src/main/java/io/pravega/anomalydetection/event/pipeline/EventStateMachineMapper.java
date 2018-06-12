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

import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.EventStateMachine;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class EventStateMachineMapper extends RichFlatMapFunction<Event, Event.Alert> {

	private transient ValueState<EventStateMachine.State> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<EventStateMachine.State> descriptor = new ValueStateDescriptor<>(
				"state", // the state name
				TypeInformation.of(EventStateMachine.State.class)); // type information
		state = getRuntimeContext().getState(descriptor);
	}

	@Override
	public void flatMap(Event event, Collector<Event.Alert> collector) throws Exception {
		EventStateMachine.State value = state.value();
		if(value == null) {
			value = EventStateMachine.Transitions.initialState;
		}

		EventStateMachine.State nextValue = value.transition(event.getEvent());
		if(nextValue instanceof EventStateMachine.InvalidTransition) {
			Event.Alert alert = new Event.Alert(Instant.now(), event, value);
			collector.collect(alert);
			state.update(null);
		} else if (!nextValue.terminal()){
			state.update(nextValue);
		} else {
			state.update(null);
		}
	}
}
