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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US);

    private int rideId;
    private int vendorId;
    private LocalDateTime pickupTime;
    private LocalDateTime dropOffTime;
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

    public static DataType[] getTypeInformation() {
        DataType[] typeInfo = new DataType[] {
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.TIMESTAMP(3),
                DataTypes.TIMESTAMP(3),
                DataTypes.INT(),
                DataTypes.FLOAT(),
                DataTypes.INT(),
                DataTypes.INT(),

                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING()
        };
        return typeInfo;
    }

    public static TableSchema getTableSchema() {
        return TableSchema.builder().fields(getFieldNames(), getTypeInformation()).build();
    }

    public static Schema getSchemaWithPickupTimeAsRowTime() {
        return new Schema()
                .field("rideId", DataTypes.INT())
                .field("vendorId", DataTypes.INT())
                .field("dropOffTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .field("passengerCount", DataTypes.INT())
                .field("tripDistance", DataTypes.FLOAT())
                .field("startLocationId", DataTypes.INT())
                .field("destLocationId", DataTypes.INT())
                .field("startLocationBorough", DataTypes.STRING())
                .field("startLocationZone", DataTypes.STRING())
                .field("startLocationServiceZone", DataTypes.STRING())
                .field("destLocationBorough", DataTypes.STRING())
                .field("destLocationZone", DataTypes.STRING())
                .field("destLocationServiceZone", DataTypes.STRING())
                .field("pickupTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .rowtime(new Rowtime().timestampsFromField("pickupTime").watermarksPeriodicBounded(30000L));
    }

    public static Schema getSchemaWithDropOffTimeAsRowTime() {
        return new Schema()
                .field("rideId", DataTypes.INT())
                .field("vendorId", DataTypes.INT())
                .field("pickupTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .field("passengerCount", DataTypes.INT())
                .field("tripDistance", DataTypes.FLOAT())
                .field("startLocationId", DataTypes.INT())
                .field("destLocationId", DataTypes.INT())
                .field("startLocationBorough", DataTypes.STRING())
                .field("startLocationZone", DataTypes.STRING())
                .field("startLocationServiceZone", DataTypes.STRING())
                .field("destLocationBorough", DataTypes.STRING())
                .field("destLocationZone", DataTypes.STRING())
                .field("destLocationServiceZone", DataTypes.STRING())
                .field("dropOffTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .rowtime(new Rowtime().timestampsFromField("pickupTime").watermarksPeriodicBounded(30000L));
    }

    public static Row transform(TripRecord tripRecord) {
        Row row = new Row(getFieldNames().length);
        row.setField(0, tripRecord.getRideId());
        row.setField(1, tripRecord.getVendorId());
        row.setField(2, tripRecord.getPickupTime());
        row.setField(3, tripRecord.getDropOffTime());
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

        int pickupTime = Long.compare(this.getPickupTime().toEpochSecond(ZoneOffset.UTC), other.getPickupTime().toEpochSecond(ZoneOffset.UTC));
        int dropOffTime = Long.compare(this.getDropOffTime().toEpochSecond(ZoneOffset.UTC), other.getDropOffTime().toEpochSecond(ZoneOffset.UTC));

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
        sb.append(TIME_FORMATTER.format(pickupTime)).append(",");
        sb.append(TIME_FORMATTER.format(dropOffTime)).append(",");
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
                builder.pickupTime(LocalDateTime.parse(data, TIME_FORMATTER));
            } else if ( offset == 3 ) {
                builder.dropOffTime(LocalDateTime.parse(data, TIME_FORMATTER));
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