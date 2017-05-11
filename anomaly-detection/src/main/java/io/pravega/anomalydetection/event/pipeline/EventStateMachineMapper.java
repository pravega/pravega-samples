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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class EventStateMachineMapper implements FlatMapFunction<Event, Event.Alert>, ListCheckpointed<HashMap<Integer,EventStateMachine.State>> {

	HashMap<Integer, EventStateMachine.State> states = new HashMap<>();

	@Override
	public void flatMap(Event event, Collector<Event.Alert> collector) throws Exception {
		EventStateMachine.State state;
		if(states.containsKey(event.getSourceAddress())) {
			state = states.remove(event.getSourceAddress());
		} else {
			state = EventStateMachine.Transitions.initialState;
		}

		EventStateMachine.State nextState = state.transition(event.getEvent());
		if(nextState instanceof EventStateMachine.InvalidTransition) {
			Event.Alert alert = new Event.Alert(Instant.now(),event, state);
			collector.collect(alert);
		} else if (!nextState.terminal()){
			states.put(event.getSourceAddress(), nextState);
		}
	}

	@Override
	public List<HashMap<Integer, EventStateMachine.State>> snapshotState(long checkpointId, long timestamp) throws Exception {
		return Collections.singletonList(states);
	}

	@Override
	public void restoreState(List<HashMap<Integer, EventStateMachine.State>> statesList) throws Exception {
		for(HashMap<Integer, EventStateMachine.State> state: statesList) {
			states.putAll(state);
		}
	}
}
