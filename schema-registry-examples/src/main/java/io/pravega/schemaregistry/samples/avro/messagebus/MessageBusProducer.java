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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.avro.generated.Type1;
import io.pravega.schemaregistry.samples.avro.generated.Type2;
import io.pravega.schemaregistry.samples.avro.generated.Type3;
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
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBusProducer {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClientConfig config;
    private final String scope;
    private final String stream;
    private final EventStreamWriter<SpecificRecordBase> writer;

    private MessageBusProducer(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        this.config = SchemaRegistryClientConfig.builder().schemaRegistryUri((URI.create(registryUri))).build();
        this.scope = scope;
        this.stream = stream;
        String groupId = NameUtils.getScopedStreamName(scope, stream);
        initialize(groupId);
        this.writer = createWriter(groupId);
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
            formatter.printHelp("avro-producer", options);
            
            System.exit(-1);
        }

        String pravegaUri = cmd.getOptionValue("pravegaUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        MessageBusProducer producer = new MessageBusProducer(pravegaUri, registryUri, scope, stream);
        
        AtomicInteger counter = new AtomicInteger();

        while (true) {
            System.out.println("choose: (1, 2 or 3)");
            System.out.println("1. Type1");
            System.out.println("2. Type2");
            System.out.println("3. Type3");
            System.out.println("> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            try {
                int choice = Integer.parseInt(s);
                switch (choice) {
                    case 1:
                        Type1 type1 = new Type1("Type1", counter.incrementAndGet());
                        producer.produce(type1).join();
                        System.out.println("Written event:" + type1);
                        break;
                    case 2:
                        Type2 type2 = new Type2("Type2", counter.incrementAndGet(), "field2");
                        producer.produce(type2).join();
                        System.out.println("Written event:" + type2);
                        break;
                    case 3:
                        Type3 type3 = new Type3("Type3", counter.incrementAndGet(), "field2", "field3");
                        producer.produce(type3).join();
                        System.out.println("Written event:" + type3);
                        break;
                    default:
                        System.err.println("invalid choice!");
                }
            } catch (Exception e) {
                System.err.println("invalid choice!");
            }
        }
    }
    
    private void initialize(String groupId) {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
    }

    private EventStreamWriter<SpecificRecordBase> createWriter(String groupId) {
        AvroSchema<SpecificRecordBase> schema1 = AvroSchema.ofSpecificRecord(Type1.class);
        AvroSchema<SpecificRecordBase> schema2 = AvroSchema.ofSpecificRecord(Type2.class);
        AvroSchema<SpecificRecordBase> schema3 = AvroSchema.ofSpecificRecord(Type3.class);

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(SerializationFormat.Avro,true)
                                                            .registerSchema(true)
                                                            .registryConfig(config)
                                                            .build();

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Type1.class, schema1);
        map.put(Type2.class, schema2);
        map.put(Type3.class, schema3);
        Serializer<SpecificRecordBase> serializer = SerializerFactory.avroMultiTypeSerializer(serializerConfig, map);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(SpecificRecordBase event) {
        return writer.writeEvent(event);
    }
}
