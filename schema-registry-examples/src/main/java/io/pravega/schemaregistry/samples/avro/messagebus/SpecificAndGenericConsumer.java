/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.avro.messagebus;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.avro.generated.Type1;
import io.pravega.schemaregistry.samples.avro.generated.Type2;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
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

public class SpecificAndGenericConsumer {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamReader<Either<SpecificRecordBase, Object>> reader;

    private SpecificAndGenericConsumer(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri((URI.create(registryUri))).build();
        client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        this.scope = scope;
        this.stream = stream;
        String groupId = NameUtils.getScopedStreamName(scope, stream);
        initialize();
        this.reader = createReader(groupId);
    }

    public static void main(String[] args) {
        Options options = new Options();

        Option pravegaUriOpt = new Option("p", "pravegaUri", true, "Controller Uri");
        pravegaUriOpt.setRequired(true);
        options.addOption(pravegaUriOpt);

        Option registryUriOpt = new Option("r", "registryUri", true, "Registry Uri");
        registryUriOpt.setRequired(true);
        options.addOption(registryUriOpt);

        Option scopeOpt = new Option("sc", "scope", true, "scope");
        scopeOpt.setRequired(true);
        options.addOption(scopeOpt);

        Option streamOpt = new Option("st", "stream", true, "stream");
        streamOpt.setRequired(true);
        options.addOption(streamOpt);

        CommandLineParser parser = new BasicParser();
        
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("avro-consumer", options);
            
            System.exit(-1);
        }

        String pravegaUri = cmd.getOptionValue("pravegaUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        SpecificAndGenericConsumer consumer = new SpecificAndGenericConsumer(pravegaUri, registryUri, scope, stream);
        
        while (true) {
            EventRead<Either<SpecificRecordBase, Object>> event = consumer.consume();
            if (event.getEvent() != null) {
                Either<SpecificRecordBase, Object> record = event.getEvent();
                if (record.isLeft()) {
                    if (record.getLeft() instanceof Type1) {
                        Type1 type1 = (Type1) record.getLeft();
                        System.err.println("processing record of type Type1: " + type1);
                    } else if (record.getLeft() instanceof Type2) {
                        Type2 type2 = (Type2) record.getLeft();
                        System.err.println("processing record of type Type2: " + type2);
                    } else {
                        throw new IllegalArgumentException(" we should not be here ");
                    }
                } else {
                    Object rec = record.getRight();
                    System.err.println("processing generic record " + rec);
                }
            }
        }
    }
    
    private void initialize() {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
    }

    private EventStreamReader<Either<SpecificRecordBase, Object>> createReader(String groupId) {
        AvroSchema<SpecificRecordBase> schema1 = AvroSchema.ofSpecificRecord(Type1.class);
        AvroSchema<SpecificRecordBase> schema2 = AvroSchema.ofSpecificRecord(Type2.class);

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(SerializationFormat.Avro, true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Type1.class, schema1);
        map.put(Type2.class, schema2);
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<Either<SpecificRecordBase, Object>> deserializer = SerializerFactory.avroTypedOrGenericDeserializer(serializerConfig, map);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        // endregion
    }

    private EventRead<Either<SpecificRecordBase, Object>> consume() {
        return reader.readNextEvent(1000);
    }
}
