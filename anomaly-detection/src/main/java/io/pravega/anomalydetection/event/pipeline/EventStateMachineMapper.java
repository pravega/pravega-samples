/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.state.Event;
import io.pravega.anomalydetection.event.state.EventStateMachine;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;

public class EventStateMachineMapper implements FlatMapFunction<Event, Event.Alert>, Checkpointed<HashMap<Integer,EventStateMachine.State>> {

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
	public HashMap<Integer, EventStateMachine.State> snapshotState(long l, long l1) throws Exception {
		return states;
	}

	@Override
	public void restoreState(HashMap<Integer, EventStateMachine.State> stateMap) throws Exception {
		states.putAll(stateMap);
	}
}
