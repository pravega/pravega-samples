/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.json.objects.Address;
import io.pravega.schemaregistry.samples.json.objects.DerivedUser1;
import io.pravega.schemaregistry.samples.json.objects.DerivedUser2;
import io.pravega.schemaregistry.samples.json.objects.User;
import io.pravega.schemaregistry.serializer.json.impl.JsonSerializerFactory;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.serializers.WithSchema;
import io.pravega.shared.NameUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Sample class that demonstrates how to use Json Serializers and Deserializers provided by Schema registry's 
 * {@link SerializerFactory}.
 * This class has multiple deserialization options 
 * 1. Deserialize into java class (schema on read).
 * 2. Deserialize into {@link Map}. No schema. 
 * 3. Deserialize into {@link Map} while retrieving writer schema. 
 * 4. Multiplexed Deserializer that deserializes data into one of java objects based on {@link SchemaInfo#type}.
 */
public class JsonDemo {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;

    private JsonDemo(String pravegaUri, String registryUri) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaUri)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(registryUri)).build();
        client = SchemaRegistryClientFactory.withDefaultNamespace(config);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option pravegaUriOpt = new Option("p", "pravegaUri", true, "Pravega Uri");
        pravegaUriOpt.setRequired(true);
        options.addOption(pravegaUriOpt);

        Option registryUriOpt = new Option("r", "registryUri", true, "Registry Uri");
        registryUriOpt.setRequired(true);
        options.addOption(registryUriOpt);

        CommandLineParser parser = new BasicParser();

        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("-p pravegaUri -r registryUri", options);

            System.exit(-1);
        }

        String pravegaUri = cmd.getOptionValue("pravegaUri");
        String registryUri = cmd.getOptionValue("registryUri");

        JsonDemo demo = new JsonDemo(pravegaUri, registryUri);

        demo.demo();
        demo.demoMultipleEventTypesInStream();

        System.exit(0);
    }

    private void demo() throws JsonProcessingException {
        // create stream
        String scope = "scope";
        String stream = "json";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Json;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    false, ImmutableMap.of()));

            JSONSchema<DerivedUser2> schema = JSONSchema.of(DerivedUser2.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Serializer<DerivedUser2> serializer = JsonSerializerFactory.serializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<DerivedUser2> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new DerivedUser2("name", new Address("street", "city"), 30, "user2"));

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig))) {
                String readerGroupName = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(readerGroupName,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<DerivedUser2> deserializer = JsonSerializerFactory.deserializer(serializerConfig, schema);

                EventStreamReader<DerivedUser2> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

                EventRead<DerivedUser2> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;

                // endregion

                // region generic read
                String rg2 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<JsonNode> genericDeserializer = JsonSerializerFactory.genericDeserializer(serializerConfig);

                EventStreamReader<JsonNode> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<JsonNode> event2 = reader2.readNextEvent(1000);
                assert event2.getEvent() != null;
                JsonNode obj = event2.getEvent();
                // endregion
            }
        }
    }

    private void demoMultipleEventTypesInStream() throws JsonProcessingException {
        // create stream
        String scope = "scope";
        String stream = "jsonmultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Json;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    true));

            JSONSchema<User> schema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
            JSONSchema<User> schema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Map<Class<? extends User>, JSONSchema<User>> map = new HashMap<>();
            map.put(DerivedUser1.class, schema1);
            map.put(DerivedUser2.class, schema2);
            Serializer<User> serializer = SerializerFactory.jsonMultiTypeSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<User> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new DerivedUser2());
            writer.writeEvent(new DerivedUser1());

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig))) {
                String rg = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<User> deserializer = SerializerFactory.jsonMultiTypeDeserializer(serializerConfig, map);

                EventStreamReader<User> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<User> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                assert event.getEvent() instanceof DerivedUser2;
                event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                assert event.getEvent() instanceof DerivedUser1;
                // endregion

                // region read into writer schema
                String rg2 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<JsonNode> genericDeserializer = JsonSerializerFactory.genericDeserializer(serializerConfig);

                EventStreamReader<JsonNode> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<JsonNode> genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends User>, JSONSchema<User>> map2 = new HashMap<>();
                // add only one schema
                map2.put(DerivedUser1.class, schema1);

                Serializer<Either<User, WithSchema<JsonNode>>> eitherDeserializer =
                        SerializerFactory.jsonTypedOrGenericDeserializer(serializerConfig, map2);

                EventStreamReader<Either<User, WithSchema<JsonNode>>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<User, WithSchema<JsonNode>>> e1 = reader3.readNextEvent(1000);
                assert e1.getEvent() != null;
                assert e1.getEvent().isRight();

                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof DerivedUser1;
                //endregion
            }
        }
    }
}