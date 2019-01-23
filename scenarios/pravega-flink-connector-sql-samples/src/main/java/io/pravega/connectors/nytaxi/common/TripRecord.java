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

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Locale;

/**
 * The trip record data set contains following elements. It has total 17 columns of which we are extracting only some of them.
 * Sample record
 * 2,2018-01-30 17:51:30,2018-01-30 18:10:37,2,2.09,1,N,186,229,1,13,1,0.5,2.96,0,0.3,17.76
 * column1:  vendorId
 * column2:  pickupTime
 * column3:  dropOffTime
 * column4:  passengerCount
 * column5:  tripDistance
 * column6:  RateCodeID //ignore
 * column7:  Store_and_fwd_flag //ignore
 * column8:  PULocationID //start location id
 * column9:  DOLocationID //drop off location id
 * column10 and above //ignore
 */
@Builder
@Data
public final class TripRecord implements Serializable, Comparable<TripRecord> {

    public static transient DateTimeFormatter TIME_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    private int rideId;
    private int vendorId;
    private DateTime pickupTime;
    private DateTime dropOffTime;
    private int passengerCount;
    private float tripDistance;
    private int startLocationId;
    private int destLocationId;

    private String startLocationBorough;
    private String startLocationZone;
    private String startLocationServiceZone;
    private String destLocationBorough;
    private String destLocationZone;
    private String destLocationServiceZone;

    public static String[] getFieldNames() {
        return new String[] {
                "rideId",
                "vendorId",
                "pickupTime",
                "dropOffTime",
                "passengerCount",
                "tripDistance",
                "startLocationId",
                "destLocationId",

                "startLocationBorough",
                "startLocationZone",
                "startLocationServiceZone",
                "destLocationBorough",
                "destLocationZone",
                "destLocationServiceZone"
        };
    }

    public static TypeInformation[] getTypeInformation() {
        TypeInformation<?>[] typeInfo = new TypeInformation[] {
                Types.INT,
                Types.INT,
                Types.SQL_TIMESTAMP,
                Types.SQL_TIMESTAMP,
                Types.INT,
                Types.FLOAT,
                Types.INT,
                Types.INT,

                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING
        };
        return typeInfo;
    }

    public static TableSchema getTableSchema() {
        return new TableSchema(getFieldNames(), getTypeInformation());
    }

    public static Row transform(TripRecord tripRecord) {
        Row row = new Row(getFieldNames().length);
        row.setField(0, tripRecord.getRideId());
        row.setField(1, tripRecord.getVendorId());
        row.setField(2, Timestamp.valueOf(tripRecord.getPickupTime().toString(TIME_FORMATTER)));
        row.setField(3, Timestamp.valueOf(tripRecord.getDropOffTime().toString(TIME_FORMATTER)));
        row.setField(4, tripRecord.getPassengerCount());
        row.setField(5, tripRecord.getTripDistance());
        row.setField(6, tripRecord.getStartLocationId());
        row.setField(7, tripRecord.getDestLocationId());
        row.setField(8, tripRecord.getStartLocationBorough());
        row.setField(9, tripRecord.getStartLocationZone());
        row.setField(10, tripRecord.getStartLocationServiceZone());
        row.setField(11, tripRecord.getDestLocationBorough());
        row.setField(12, tripRecord.getDestLocationZone());
        row.setField(13, tripRecord.getDestLocationServiceZone());
        return row;
    }

    @Override
    public int compareTo(TripRecord other) {
        if (other == null) {
            return 1;
        }

        int pickupTime = Long.compare(this.getPickupTime().getMillis(), other.getPickupTime().getMillis());
        int dropOffTime = Long.compare(this.getDropOffTime().getMillis(), other.getDropOffTime().getMillis());

        if (pickupTime == 0) {
            return dropOffTime;
        }
        return pickupTime;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TripRecord &&
                this.rideId == ((TripRecord) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(vendorId).append(",");
        sb.append(pickupTime.toString(TIME_FORMATTER)).append(",");
        sb.append(dropOffTime.toString(TIME_FORMATTER)).append(",");
        sb.append(passengerCount).append(",");
        sb.append(tripDistance).append(",");
        sb.append(startLocationId).append(",");
        sb.append(destLocationId).append(",");
        sb.append(startLocationBorough).append(",");
        sb.append(startLocationZone).append(",");
        sb.append(startLocationServiceZone).append(",");
        sb.append(destLocationBorough).append(",");
        sb.append(destLocationZone).append(",");
        sb.append(destLocationServiceZone);

        return sb.toString();
    }

    public static TripRecord parse(String line) {

        String[] tokens = line.split(",");

        if (tokens.length != 17) {
            throw new RuntimeException("Invalid Trip record: " + line);
        }

        TripRecordBuilder builder = new TripRecordBuilder();

        int offset = 1;

        for (String data: tokens) {

            if ( offset == 6 || offset == 7 ) {
                offset++;
                continue;
            } else if ( offset > 9 ) {
                break;
            }

            if ( offset == 1 ) {
                builder.vendorId(Integer.parseInt(data));
            } else if ( offset == 2 ) {
                builder.pickupTime(TIME_FORMATTER.parseDateTime(data));
            } else if ( offset == 3 ) {
                builder.dropOffTime(TIME_FORMATTER.parseDateTime(data));
            } else if ( offset == 4 ) {
                builder.passengerCount(Integer.parseInt(data));
            } else if ( offset == 5 ) {
                builder.tripDistance(Float.parseFloat(data));
            } else if ( offset == 8 ) {
                builder.startLocationId(Integer.parseInt(data));
            } else if ( offset == 9 ) {
                builder.destLocationId(Integer.parseInt(data));
            }

            offset++;
        }

        return builder.build();
    }

}