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

package io.pravega.connectors.nytaxi.common;

import java.io.Serializable;
import java.util.Locale;

import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Data
@Builder
public class RideInfo implements Comparable<RideInfo>, Serializable {

    private long rideId;
    private boolean isStart;
    private DateTime startTime;
    private DateTime endTime;
    private float startLon;
    private float startLat;
    private float endLon;
    private float endLat;
    private short passengerCnt;
    private long taxiId;
    private long driverId;

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    @Override
    public int compareTo(RideInfo other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            }
            else {
                if (this.isStart) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
        }
        else {
            return compareTimes;
        }
    }

    public long getEventTime() {
        if (isStart) {
            return startTime.getMillis();
        }
        else {
            return endTime.getMillis();
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RideInfo &&
                this.rideId == ((RideInfo) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(isStart ? "START" : "END").append(",");
        sb.append(startTime.toString(timeFormatter)).append(",");
        sb.append(endTime.toString(timeFormatter)).append(",");
        sb.append(startLon).append(",");
        sb.append(startLat).append(",");
        sb.append(endLon).append(",");
        sb.append(endLat).append(",");
        sb.append(passengerCnt).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId);

        return sb.toString();
    }

}
