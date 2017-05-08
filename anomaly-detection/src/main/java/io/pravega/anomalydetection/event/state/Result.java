/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package io.pravega.anomalydetection.event.state;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Result implements Serializable {

	private String networkId;
	private List<String> ipAddress = new ArrayList<>();
	private int count;
	private long minTimestamp;
	private long maxTimestamp;
	private String location;
	private String minTimestampAsString;
	private String maxTimestampAsString;


	public String getNetworkId() {
		return networkId;
	}

	public void setNetworkId(String networkId) {
		this.networkId = networkId;
	}

	public List<String> getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(List<String> ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public long getMinTimestamp() {
		return minTimestamp;
	}

	public void setMinTimestamp(long minTimestamp) {
		this.minTimestamp = minTimestamp;
		this.minTimestampAsString = getDateTimeAsString(minTimestamp);
	}

	public long getMaxTimestamp() {
		return maxTimestamp;
	}

	public void setMaxTimestamp(long maxTimestamp) {
		this.maxTimestamp = maxTimestamp;
		this.maxTimestampAsString = getDateTimeAsString(maxTimestamp);
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getMinTimestampAsString() {
		return minTimestampAsString;
	}

	public void setMinTimestampAsString(String minTimestampAsString) {
		this.minTimestampAsString = minTimestampAsString;
	}

	public String getMaxTimestampAsString() {
		return maxTimestampAsString;
	}

	public void setMaxTimestampAsString(String maxTimestampAsString) {
		this.maxTimestampAsString = maxTimestampAsString;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Network ID: ").append(networkId).append(" ");
		builder.append("Hosts: ").append(ipAddress).append(" ");
		builder.append("Total: ").append(count).append(" ");
		builder.append("Start Time: [").append(new Date(minTimestamp)).append("] ");
		builder.append("End Time: [").append(new Date(maxTimestamp)).append("] ");
		builder.append("Location: [").append(location).append("] ");
		return builder.toString();
	}

	private String getDateTimeAsString(long dateTime) {
		return Instant.ofEpochMilli(dateTime).toString();
	}
}
