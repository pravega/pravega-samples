package io.pravega.examples.flink.primer.util;


import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.NumberSerializers;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

public class FlinkPravegaHelper extends FlinkPravegaParams {
    public FlinkPravegaHelper(ParameterTool param) {
        super(param);
    }

    // This should be added to FlinkPravegaParams
    /**
     * Constructs a new writer using stream/scope name from job parameters.
     *
     * @param stream Stream to read from.
     * @param serializationSchema The implementation for serializing every event into pravega's storage format.
     * @param router The implementation to extract the partition key from the event.
     * @param <T> Type for events on this stream.
     * @param txnTimeoutMillis      The number of milliseconds after which the transaction will be aborted.
     * @param txnGracePeriodMillis  The maximum amount of time, in milliseconds, until which transaction may
     *                              remain active, after a scale operation has been initiated on the underlying stream.
     */
    public <T extends Serializable> FlinkPravegaWriter<T> newWriter(final StreamId stream,
                                                                    final SerializationSchema<T> serializationSchema,
                                                                    final PravegaEventRouter<T> router,
                                                                    final long txnTimeoutMillis,
                                                                    final long txnGracePeriodMillis) {
        return new FlinkPravegaWriter<>(
                getControllerUri(),
                stream.getScope(),
                stream.getName(),
                serializationSchema,
                router,
                txnTimeoutMillis,
                txnGracePeriodMillis
        );
    }

    // This should be added to FlinkPravegaParams
    /**
     * Constructs a new writer using stream/scope name from job parameters.
     *
     * @param stream Stream to read from.
     * @param eventType Class type for events on this stream
     * @param router The implementation to extract the partition key from the event.
     * @param <T> Type for events on this stream.
     * @param txnTimeoutMillis      The number of milliseconds after which the transaction will be aborted.
     * @param txnGracePeriodMillis  The maximum amount of time, in milliseconds, until which transaction may
     *                              remain active, after a scale operation has been initiated on the underlying stream.
     */
    public <T extends Serializable> FlinkPravegaWriter<T> newWriter(final StreamId stream,
                                                                    final Class<T> eventType,
                                                                    final PravegaEventRouter<T> router,
                                                                    final long txnTimeoutMillis,
                                                                    final long txnGracePeriodMillis) {
        return new FlinkPravegaWriter<>(
            getControllerUri(),
            stream.getScope(),
            stream.getName(),
            PravegaSerialization.serializationFor(eventType),
            router,
            txnTimeoutMillis,
            txnGracePeriodMillis
        );
    }

}
