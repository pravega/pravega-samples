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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Slf4j
public class Helper {

    public void createStream(final Stream stream,
                             final ClientConfig clientConfig,
                             final StreamConfiguration streamConfig) {

        StreamManager streamManager = StreamManager.create(clientConfig);

        boolean scopeResult = streamManager.createScope(stream.getScope());
        if (scopeResult) {
            log.info("created scope: {}" , stream.getScope());
        } else {
            log.warn("scope: {} already exists" , stream.getScope());
        }

        boolean streamResult = streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        if (streamResult) {
            log.info("created stream: {}" , stream.getStreamName());
        } else {
            log.warn("stream: {} already exists" , stream.getStreamName());
        }

    }

    public Map<Integer, ZoneLookup> parseZoneData(String taxiZoneLookupFilePath) throws IOException {
        Map<Integer, ZoneLookup> map = new HashMap<>();
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (
                InputStream is = classloader.getResourceAsStream(taxiZoneLookupFilePath);
                GZIPInputStream gzipInputStream = new GZIPInputStream(is);
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream, "UTF-8"));
        )
        {
            String line;
            boolean start = true;
            while (reader.ready() && (line = reader.readLine()) != null) {
                if (start) {
                    start = false;
                    continue;
                }
                ZoneLookup zoneLookup = ZoneLookup.parse(line);
                map.put(zoneLookup.getLocationId(), zoneLookup);
            }
        }
        return map;
    }

}
