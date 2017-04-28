/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.event.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Result implements Serializable {

	private String networkId;
	private List<String> ipAddress = new ArrayList<>();
	private int count;
	private long minTimestamp;
	private long maxTimestamp;

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
	}

	public long getMaxTimestamp() {
		return maxTimestamp;
	}

	public void setMaxTimestamp(long maxTimestamp) {
		this.maxTimestamp = maxTimestamp;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Network ID: ").append(networkId).append(" ");
		builder.append("Hosts: ").append(ipAddress).append(" ");
		builder.append("Total: ").append(count).append(" ");
		builder.append("Start Time: [").append(new Date(minTimestamp)).append("] ");
		builder.append("End Time: [").append(new Date(maxTimestamp)).append("] ");
		return builder.toString();
	}
}
