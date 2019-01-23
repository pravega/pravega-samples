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

package io.pravega.connectors.nytaxi.source;

import io.pravega.connectors.nytaxi.common.TripRecord;
import io.pravega.connectors.nytaxi.common.ZoneLookup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Slf4j
public class TaxiDataSource implements SourceFunction<TripRecord> {

    private final String tripDataFilePath;
    private final Map<Integer, ZoneLookup> zoneLookupRecordMap;
    private int limit = Integer.MAX_VALUE;

    public TaxiDataSource(String tripDataFilePath, Map<Integer, ZoneLookup> zoneLookupRecordMap) {
        this.tripDataFilePath = tripDataFilePath;
        this.zoneLookupRecordMap = zoneLookupRecordMap;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public void run(SourceContext<TripRecord> sourceContext) throws Exception {

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (
                InputStream is = classloader.getResourceAsStream(tripDataFilePath);
                GZIPInputStream gzipInputStream = new GZIPInputStream(is);
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream, "UTF-8"));
        )
        {
            int count = 0;
            String line;
            TripRecord tripRecord;
            boolean start = true;
            int rideId = 1;
            while (reader.ready() && (line = reader.readLine()) != null) {
                if (start) {
                    start = false;
                    continue;
                }
                if (count == limit) { break; }

                // read first ride
                tripRecord = TripRecord.parse(line);
                tripRecord.setRideId(rideId++);
                ZoneLookup startLocZoneLookup = zoneLookupRecordMap.get(tripRecord.getStartLocationId());
                ZoneLookup destLocZoneLookup = zoneLookupRecordMap.get(tripRecord.getDestLocationId());
                tripRecord.setStartLocationBorough(startLocZoneLookup.getBorough());
                tripRecord.setStartLocationZone(startLocZoneLookup.getZone());
                tripRecord.setStartLocationServiceZone(startLocZoneLookup.getServiceZone());
                tripRecord.setDestLocationBorough(destLocZoneLookup.getBorough());
                tripRecord.setDestLocationZone(destLocZoneLookup.getZone());
                tripRecord.setDestLocationServiceZone(destLocZoneLookup.getServiceZone());
                sourceContext.collect(tripRecord);
                count++;
            }

        }
    }

    @Override
    public void cancel() { }

}
