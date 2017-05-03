/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.state;

import java.io.Serializable;
import java.time.Instant;

public class Event implements Serializable {

	private int sourceAddress;

	private EventType event;

	private Instant eventTime;

	private String networkId;

	private LatLon latlon;

	public Event() {}

	public Event(int address, EventType event, Instant eventTime, String networkId, LatLon latlon) {
		this.sourceAddress = address;
		this.event = event;
		this.eventTime = eventTime;
		this.networkId = networkId;
		this.latlon = latlon;
	}

	public int getSourceAddress() {
		return sourceAddress;
	}

	public EventType getEvent() {
		return event;
	}

	public void setSourceAddress(int sourceAddress) {
		this.sourceAddress = sourceAddress;
	}

	public void setEvent(EventType event) {
		this.event = event;
	}

	public Instant getEventTime() {	return eventTime; }

	public void setEventTime(Instant eventTime) { this.eventTime = eventTime; }

	public String getNetworkId() {return networkId;}

	public void setNetworkId(String networkId) {this.networkId = networkId;}

	public LatLon getLatlon() {return latlon;}

	public void setLatlon(LatLon latlon) {
		this.latlon = latlon;
	}

	@Override
	public String toString() {
		return "Event: "+ eventTime + ": " + EventType.formatAddress(sourceAddress) + ": " + EventType.eventTypeName(event);
	}

	public static class EventType implements Serializable {

		public static final int a = 1;
		public static final int b = 2;
		public static final int c = 3;
		public static final int d = 4;
		public static final int e = 5;
		public static final int f = 6;
		public static final int g = 7;

		private int value;

		public EventType() {}

		public EventType(int value) {
			this.value = value;
		}

		public int getValue() { return value; }

		public void setValue(int value) {
			this.value = value;
		}

		public static String eventTypeName(EventType eventType) {
			char a = 'a';
			int val = (int) a + eventType.getValue() - 1;
			String retVal =  String.valueOf(Character.toChars(val));
			return retVal;
		}

		public static String formatAddress(int address) {
			int b1 = (address >>> 24) & 0xff;
			int b2 = (address >>> 16) & 0xff;
			int b3 = (address >>>  8) & 0xff;
			int b4 =  address         & 0xff;
			return  b1 + "." + b2 + "." + b3 + "." + b4;
		}

	}

	public static class Alert implements Serializable {

		private EventStateMachine.State state;
		private String networkId;
		private Instant alertTime;
		private Event event;

		public Alert(){}

		public Alert(Instant alertTime, Event event, EventStateMachine.State state) {
			this.alertTime = alertTime;
			this.networkId = event.getNetworkId();
			this.event = event;
			this.state = state;
		}

		public EventStateMachine.State getState() {
			return state;
		}

		public void setState(EventStateMachine.State state) {
			this.state = state;
		}

		public String getNetworkId() {
			return networkId;
		}

		public void setNetworkId(String networkId) {
			this.networkId = networkId;
		}

		public Instant getAlertTime() {
			return alertTime;
		}

		public void setAlertTime(Instant alertTime) {
			this.alertTime = alertTime;
		}

		public Event getEvent() {
			return event;
		}

		public void setEvent(Event event) {
			this.event = event;
		}

		@Override
		public String toString() {
			return "ALERT: " + networkId + ": " + alertTime + ": " + EventType.formatAddress(event.getSourceAddress()) + ": " + state.getName() + " -> " + EventType.eventTypeName(event.getEvent());
		}
	}

	public static class LatLon implements Serializable {
		private double lat;
		private double lon;

		public LatLon() {}

		public LatLon(double lat, double lon) {
			this.lat = lat;
			this.lon = lon;
		}

		public double getLat() {
			return lat;
		}

		public void setLat(double lat) {
			this.lat = lat;
		}

		public double getLon() {
			return lon;
		}

		public void setLon(double lon) {
			this.lon = lon;
		}
	}

}
