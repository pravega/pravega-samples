/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.turbineheatprocessor;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;

public class Utils {

    /**
     * Creates a Pravega stream with a given configuration.
     *
     * @param pravegaConfig the Pravega configuration.
     * @param streamName the stream name (qualified or unqualified).
     * @param streamConfig the stream configuration (scaling policy, retention policy).
     */
    public static Stream createStream(PravegaConfig pravegaConfig, String streamName, StreamConfiguration streamConfig) {
        // resolve the qualified name of the stream
        Stream stream = pravegaConfig.resolve(streamName);

        try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
            // create the requested scope (if necessary)
            streamManager.createScope(stream.getScope());

            // create the requested stream based on the given stream configuration
            streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        }

        return stream;
    }
}
