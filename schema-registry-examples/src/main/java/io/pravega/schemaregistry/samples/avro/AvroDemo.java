/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.avro;

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
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.samples.avro.generated.Type1;
import io.pravega.schemaregistry.samples.avro.generated.Type2;
import io.pravega.schemaregistry.samples.avro.generated.Type3;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.avro.impl.AvroSerializerFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.shared.NameUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecordBase;
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
 * Sample class that demonstrates how to use Avro Serializers and Deserializers provided by Schema registry's 
 * {@link SerializerFactory}.
 * Avro has multiple deserialization options 
 * 1. Deserialize into Avro generated java class (schema on read).
 * 2. Deserialize into {@link GenericRecord} using user supplied schema (schema on read). 
 * 3. Deserialize into {@link GenericRecord} while retrieving writer schema. 
 * 4. Multiplexed Deserializer that deserializes data into one of avro generated typed java objects based on {@link SchemaInfo#type}.
 */
public class AvroDemo {
    private static final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private static final Schema SCHEMA2 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("backwardPolicy compatible with schema1")
            .endRecord();

    private static final Schema SCHEMA3 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("c")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;

    public AvroDemo(String pravegaUri, String registryUri) {
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

        AvroDemo demo = new AvroDemo(pravegaUri, registryUri);
        demo.demoAvroSchemaEvolution();

        demo.demoSchemaFromReflection();
        demo.demoAvroGeneratedPOJOs();
        demo.demoMultipleEventTypesInStream();

        System.exit(0);
    }

    private void demoAvroSchemaEvolution() {
        // create stream
        String scope = "scope";
        String stream = "avroevolution";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            System.out.println("adding new group with: \nserialization format = avro\n compatibility = backwardPolicy");

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<GenericRecord> schema1 = AvroSchema.ofRecord(SCHEMA1);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();

            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            // region writer with schema1
            Serializer<GenericRecord> serializer = AvroSerializerFactory.serializer(serializerConfig, schema1);

            EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
            writer.writeEvent(record).join();
            // endregion

            System.out.println("registering schema with default value for new field:" + SCHEMA2.toString(true));

            AvroSchema<GenericRecord> schema2 = AvroSchema.ofRecord(SCHEMA2);

            // region writer with schema2
            serializer = AvroSerializerFactory.serializer(serializerConfig, schema2);

            writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
            writer.writeEvent(record).join();
            // endregion

            // region writer with schema3
            // this should throw exception as schema change is not backwardPolicy compatible.
            boolean exceptionThrown = false;
            try {
                System.out.println("registering schema " + SCHEMA3.toString(true));

                AvroSchema<GenericRecord> schema3 = AvroSchema.ofRecord(SCHEMA3);

                AvroSerializerFactory.serializer(serializerConfig, schema3);
            } catch (Exception ex) {
                exceptionThrown = true;
                System.out.println("schema registration failed with " + Exceptions.unwrap(ex));
            }
            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig,
                    new SocketConnectionFactoryImpl(clientConfig))) {
                String rg = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                System.out.println("reading all records into schema2" + SCHEMA2.toString(true));

                AvroSchema<Object> readSchema = AvroSchema.of(SCHEMA2);

                Serializer<Object> deserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, readSchema);

                EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                // read two events successfully
                EventRead<Object> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                event = reader.readNextEvent(1000);
                assert event.getEvent() != null;

                // create new reader, this time with incompatible schema3
                try (ReaderGroupManager readerGroupManager2 = new ReaderGroupManagerImpl(scope, clientConfig,
                        new SocketConnectionFactoryImpl(clientConfig))) {
                    String rg1 = UUID.randomUUID().toString();
                    readerGroupManager2.createReaderGroup(rg1,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    System.out.println("creating new reader to read using schema 3:" + SCHEMA3.toString(true));

                    readSchema = AvroSchema.of(SCHEMA3);

                    exceptionThrown = false;
                    try {
                        AvroSerializerFactory.genericDeserializer(serializerConfig, readSchema);
                    } catch (Exception ex) {
                        exceptionThrown = Exceptions.unwrap(ex) instanceof IllegalArgumentException;
                        System.out.println("schema validation failed with " + Exceptions.unwrap(ex));
                    }
                    assert exceptionThrown;

                    // endregion
                    // region read into writer schema
                    String rg2 = UUID.randomUUID().toString();
                    readerGroupManager2.createReaderGroup(rg2,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    deserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, null);

                    reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

                    event = reader.readNextEvent(1000);
                    System.out.println("event read =" + event.getEvent());
                    assert event.getEvent() != null;

                    event = reader.readNextEvent(1000);
                    System.out.println("event read =" + event.getEvent());
                    assert event.getEvent() != null;
                    // endregion
                }
            }
        }
    }

    private void demoSchemaFromReflection() {
        // create stream
        String scope = "scope";
        String stream = "avroreflect";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<MyClass> schema = AvroSchema.of(MyClass.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();

            // region writer
            Serializer<MyClass> serializer = AvroSerializerFactory.serializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<MyClass> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new MyClass("test")).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig,
                    new SocketConnectionFactoryImpl(clientConfig))) {
                String rg = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                AvroSchema<Object> readSchema = AvroSchema.of(ReflectData.get().getSchema(MyClass.class));

                Serializer<Object> deserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, readSchema);

                EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<Object> event = reader.readNextEvent(1000);
                assert null != event.getEvent();

                // endregion
                // region read into writer schema
                String rg2 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                deserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, null);

                reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

                event = reader.readNextEvent(1000);
                assert null != event.getEvent();
                // endregion
            }
        }
    }

    private void demoAvroGeneratedPOJOs() {
        // create stream
        String scope = "scope";
        String stream = "avrogenerated";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<Type1> schema = AvroSchema.of(Type1.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Serializer<Type1> serializer = AvroSerializerFactory.serializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<Type1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new Type1("test", 0)).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig,
                    new SocketConnectionFactoryImpl(clientConfig))) {
                String rg = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                AvroSchema<Type1> readSchema = AvroSchema.of(Type1.class);

                Serializer<Type1> deserializer = AvroSerializerFactory.deserializer(serializerConfig, readSchema);

                EventStreamReader<Type1> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<Type1> event = reader.readNextEvent(1000);
                assert null != event.getEvent();

                // endregion
                // region read into writer schema
                String rg2 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<Object> genericDeserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, null);

                EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<Object> event2 = reader2.readNextEvent(1000);
                assert null != event2.getEvent();
                // endregion
            }
        }
    }

    private void demoMultipleEventTypesInStream() {
        // create stream
        String scope = "scope";
        String stream = "avromultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<SpecificRecordBase> schema1 = AvroSchema.ofSpecificRecord(Type1.class);
            AvroSchema<SpecificRecordBase> schema2 = AvroSchema.ofSpecificRecord(Type2.class);
            AvroSchema<SpecificRecordBase> schema3 = AvroSchema.ofSpecificRecord(Type3.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
            map.put(Type1.class, schema1);
            map.put(Type2.class, schema2);
            map.put(Type3.class, schema3);
            Serializer<SpecificRecordBase> serializer = SerializerFactory.avroMultiTypeSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<SpecificRecordBase> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new Type1("test", 0)).join();
            writer.writeEvent(new Type2("test", 0, "test")).join();
            writer.writeEvent(new Type3("test", 0, "test", "test")).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig,
                    new SocketConnectionFactoryImpl(clientConfig))) {
                String rg = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<SpecificRecordBase> deserializer = SerializerFactory.avroMultiTypeDeserializer(serializerConfig, map);

                EventStreamReader<SpecificRecordBase> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<SpecificRecordBase> event1 = reader.readNextEvent(1000);
                assert null != event1.getEvent();
                assert event1.getEvent() instanceof Type1;
                EventRead<SpecificRecordBase> event2 = reader.readNextEvent(1000);
                assert null != event2.getEvent();
                assert event2.getEvent() instanceof Type2;
                EventRead<SpecificRecordBase> event3 = reader.readNextEvent(1000);
                assert null != event3.getEvent();
                assert event3.getEvent() instanceof Type3;

                // endregion
                // region read into writer schema
                String rg2 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<Object> genericDeserializer = AvroSerializerFactory.genericDeserializer(serializerConfig, null);

                EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<Object> genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = UUID.randomUUID().toString();
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map2 = new HashMap<>();
                // add only two schemas
                map2.put(Type1.class, schema1);
                map2.put(Type2.class, schema2);

                Serializer<Either<SpecificRecordBase, Object>> eitherDeserializer =
                        SerializerFactory.avroTypedOrGenericDeserializer(serializerConfig, map2);

                EventStreamReader<Either<SpecificRecordBase, Object>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<SpecificRecordBase, Object>> e1 = reader3.readNextEvent(1000);
                assert e1.getEvent() != null;
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof Type1;

                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof Type2;

                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isRight();
                //endregion
            }
        }
    }

    private static class MyClass {
        private final String name;

        private MyClass(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}

