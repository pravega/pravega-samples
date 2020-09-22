/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.allformatdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.json.objects.Address;
import io.pravega.schemaregistry.samples.json.objects.DerivedUser1;
import io.pravega.schemaregistry.samples.avro.generated.Type1;
import io.pravega.schemaregistry.samples.protobuf.generated.ProtobufTest;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
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
import java.util.Scanner;

/**
 * This sample writes objects of all json protobuf and avro formats into a single stream. For this the `serialization format` property
 * of the group is set as {@link SerializationFormat#Any}. 
 * During reads it uses {@link SerializerFactory#genericDeserializer} to deserialize them into generic records 
 * of each type and the reader returns the common base class {@link Object}. 
 * It also writes the events back into an output stream. 
 */
public class AllFormatDemo {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final String outputStream;

    public AllFormatDemo(ClientConfig clientConfig, SchemaRegistryClient client, String scope, String stream, String groupId) {
        this.clientConfig = clientConfig;
        this.client = client;
        this.scope = scope;
        this.stream = stream;
        this.outputStream = stream + "out";
        initialize(groupId);
    }

    private void initialize(String groupId) {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.createStream(scope, outputStream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SerializationFormat serializationFormat = SerializationFormat.Any;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(),
                true));
        client.addGroup(NameUtils.getScopedStreamName(scope, stream), new GroupProperties(serializationFormat,
                Compatibility.allowAny(),
                true));
    }

    public static void main(String[] args) throws JsonProcessingException {
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

        String scope = "scope";
        String stream = "allFormatDemo";
        String groupId = NameUtils.getScopedStreamName(scope, stream);
        String groupIdOut = NameUtils.getScopedStreamName(scope, stream + "out");

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaUri)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(registryUri)).build();
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withDefaultNamespace(config);
        AllFormatDemo demo = new AllFormatDemo(clientConfig, schemaRegistryClient, scope, stream, groupId);

        EventStreamWriter<Type1> avro = demo.createAvroWriter(groupId);
        EventStreamWriter<ProtobufTest.Message1> proto = demo.createProtobufWriter(groupId);
        EventStreamWriter<DerivedUser1> json = demo.createJsonWriter(groupId);
        EventStreamReader<WithSchema<Object>> reader = demo.createReader(groupId);
        
        EventStreamReader<String> outReader = demo.createReaderOutputStream(groupIdOut);
        EventStreamWriter<WithSchema<Object>> genericWriter = demo.createOutputStreamWriter(groupIdOut);

        while (true) {
            System.out.println("choose: (1, 2 or 3)");
            System.out.println("1. write avro message");
            System.out.println("2. write protobuf message");
            System.out.println("3. write json message");
            System.out.println("4. read all messages and write to output stream");
            System.out.println("5. read all messages from output stream");
            System.out.print("> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            try {
                int choice = Integer.parseInt(s);
                switch (choice) {
                    case 1:
                        avro.writeEvent(new Type1("a", 1)).join();
                        break;
                    case 2:
                        ProtobufTest.Message1 type1 = ProtobufTest.Message1.newBuilder().setName("test")
                                                                           .setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val3).build())
                                                                           .build();
                        proto.writeEvent(type1).join();
                        break;
                    case 3:
                        json.writeEvent(new DerivedUser1("json", new Address("a", "b"), 1, "users")).join();
                        break;
                    case 4:
                        EventRead<WithSchema<Object>> event = reader.readNextEvent(1000);
                        while (event.getEvent() != null || event.isCheckpoint()) {
                            System.out.println("event read:" + event.getEvent().getObject());
                            // write the event into the output branch
                            genericWriter.writeEvent(event.getEvent()).join();
                            event = reader.readNextEvent(1000);
                        }
                        break;
                    case 5:
                        EventRead<String> event2 = outReader.readNextEvent(1000);
                        while (event2.getEvent() != null || event2.isCheckpoint()) {
                            System.out.println("event read:" + event2.getEvent());
                            
                            event2 = outReader.readNextEvent(1000);
                        }
                        break;
                    default:
                }
            } catch (NumberFormatException e) {
                System.err.println("invalid choice");
            }
        }
    }
    
    private EventStreamReader<WithSchema<Object>> createReader(String groupId) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        Serializer<WithSchema<Object>> deserializer = SerializerFactory.deserializerWithSchema(serializerConfig);
        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
    }

    private EventStreamWriter<WithSchema<Object>> createOutputStreamWriter(String groupId) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        Serializer<WithSchema<Object>> serializer = SerializerFactory.serializerWithSchema(serializerConfig);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory
                .createEventWriter(outputStream, serializer, EventWriterConfig.builder().build());
    }

    private EventStreamReader<String> createReaderOutputStream(String groupId) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        Serializer<String> deserializer = SerializerFactory.deserializeAsJsonString(serializerConfig);
        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
    }

    private EventStreamWriter<Type1> createAvroWriter(String groupId) {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        AvroSchema<Type1> avro = AvroSchema.of(Type1.class);
        Serializer<Type1> serializer = SerializerFactory.avroSerializer(serializerConfig, avro);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private EventStreamWriter<DerivedUser1> createJsonWriter(String groupId) throws JsonProcessingException {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        JSONSchema<DerivedUser1> avro = JSONSchema.of(DerivedUser1.class);
        Serializer<DerivedUser1> serializer = SerializerFactory.jsonSerializer(serializerConfig, avro);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private EventStreamWriter<ProtobufTest.Message1> createProtobufWriter(String groupId) {
        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Serializer<ProtobufTest.Message1> serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }
}
