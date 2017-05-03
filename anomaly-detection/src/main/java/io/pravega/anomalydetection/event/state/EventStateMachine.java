/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.state;


import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;

public class EventStateMachine {

	public EventStateMachine() {}

	public static class Transition implements Serializable {
		private Event.EventType event;
		private State targetState;
		private float prob;

		public Transition(Event.EventType event, State targetState, float prob) {
			this.event = event;
			this.targetState = targetState;
			this.prob = prob;
		}
	}

	public static class State implements Serializable {

		private String name;

		private List<Transition> transitions;

		public State(String name) {
			this.name = name;
		}

		public State(String name, List<Transition> transitions) {
			this.name = name;
			this.transitions = transitions;
		}

		public String getName() { return  name; }

		@Override
		public String toString() {
			return name;
		}

		public boolean terminal() {
			return transitions == null || transitions.isEmpty();
		}

		public State transition(final Event.EventType eventType) {
			Optional<State> optionalState = transitions
					.stream()
					.filter(t -> t.event.getValue() == eventType.getValue())
					.map(m -> m.targetState)
					.findAny();

			if (optionalState.isPresent()) {
				return optionalState.get();
			} else {
				return new InvalidTransition("Invalid Transition");
			}
		}

		public Tuple2<Event.EventType, State> randomTransition(Random rnd) {
			if (transitions.isEmpty()) {
				throw new RuntimeException("Cannot transition from state " + name);
			}
			float p = rnd.nextFloat();
			float mass = 0.0f;
			Transition transition = null;

			for (Transition t : transitions) {
				mass += t.prob;
				if (transition == null && p <= mass) {
					transition = t;
				}
			}
			return new Tuple2<>(transition.event, transition.targetState);
		}

		public Event.EventType randomInvalidTransition(Random rnd) {
			Event.EventType value = new Event.EventType(-1);
			while (value.getValue() == -1) {
				Event.EventType g = new Event.EventType(Event.EventType.g);
				int candidate = rnd.nextInt(g.getValue() + 1);
				State transitionState = transition(new Event.EventType(candidate));
				if(transitionState instanceof InvalidTransition) {
					value = new Event.EventType(candidate);
					break;
				}
			}
			return value;
		}

	}

	public static class InvalidTransition extends State {
		public InvalidTransition(String name) {
			super(name);
		}

		public InvalidTransition(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class TerminalState extends State {
		public TerminalState(String name) {
			super(name);
		}

		public TerminalState(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class W extends State {
		public W(String name) {
			super(name);
		}

		public W(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class X extends State {
		public X(String name) {
			super(name);
		}

		public X(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class Y extends State {
		public Y(String name) {
			super(name);
		}

		public Y(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class Z extends State {
		public Z(String name) {
			super(name);
		}

		public Z(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	public static class InitialState extends State {
		public InitialState(String name) {
			super(name);
		}

		public InitialState(String name, List<Transition> transitions) {
			super(name, transitions);
		}
	}

	/*
	 State Machine Transitions
	 -------------------------

           +--<a>--> W --<b>--> Y --<e>---+
           |                    ^         |     +-----<g>---> TERM
   	 INITIAL-+                    |         |     |
           |                    |         +--> (Z)---<f>-+
           +--<c>--> X --<b>----+         |     ^        |
                     |                    |     |        |
                     +--------<d>---------+     +--------+
	 */
	public static class Transitions {
		public final static Transition transitionTerm = new Transition(new Event.EventType(Event.EventType.g), new TerminalState("Terminal"), 1.0f);
		public final static Z z = new Z("Z", Arrays.asList(transitionTerm));

		public final static Transition transitionZ2 = new Transition(new Event.EventType(Event.EventType.e), z, 1.0f);
		public final static Y y = new Y("Y", Arrays.asList(transitionZ2));

		public final static Transition transitionY2 = new Transition(new Event.EventType(Event.EventType.b), y, 0.2f);
		public final static Transition transitionZ1 = new Transition(new Event.EventType(Event.EventType.d), z, 0.8f);
		public final static X x = new X("X", Arrays.asList(transitionY2, transitionZ1));

		public final static Transition transitionY1 = new Transition(new Event.EventType(Event.EventType.b), y, 1.0f);
		public final static W w = new W("W", Arrays.asList(transitionY1));

		public final static Transition transitionW = new Transition(new Event.EventType(Event.EventType.a), w, 0.6f);
		public final static Transition transitionX = new Transition(new Event.EventType(Event.EventType.c), x, 0.4f);
		public final static InitialState initialState = new InitialState("Initial", Arrays.asList(transitionW, transitionX));
	}

	public static void main(String[] args) {
		System.out.println(Transitions.z.transition(new Event.EventType(0)));
		if(Transitions.z.transition(new Event.EventType(0)) instanceof InvalidTransition) {
			System.out.println("Its invalid transition");
		}
	}

}

