/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.codec;

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
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.codec.Codec;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * A sample class to demonstrate different kind of compression encodings that can be used with Pravega serializers.
 * It has samples for gzip, snappy, none and custom encodings. 
 * This highlights the power of encoding headers with pravega payload where schema registry facilitates clients recording
 * different encoding schemes used for encoding the data into the header used before the data. 
 */
@Slf4j
public class CompressionDemo {
    private static final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    private static final CodecType MYCOMPRESSION = new CodecType("mycompression");
    private static final Codec MY_CODEC = new Codec() {
        @Override
        public String getName() {
            return MYCOMPRESSION.getName();
        }

        @Override
        public CodecType getCodecType() {
            return MYCOMPRESSION;
        }

        @Override
        public void encode(ByteBuffer data, ByteArrayOutputStream bos) throws IOException {
            // left rotate by 1 byte
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i; 
            byte temp = array[0];
            for (i = 0; i < array.length - 1; i++) {
                array[i] = array[i + 1];
            }
            array[array.length - 1] = temp;
            bos.write(array, 0, array.length);
        }

        @Override
        public ByteBuffer decode(ByteBuffer data, Map<String, String> properties) throws IOException {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i;
            byte temp = array[array.length - 1];
            for (i = array.length - 1; i > 0; i--) {
                array[i] = array[i - 1];
            }
            array[0] = temp;
            return ByteBuffer.wrap(array);
        }
    };
    private static final Random RANDOM = new Random();

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final String groupId;
    private final AvroSchema<GenericRecord> schema1;
    private final EventStreamClientFactory clientFactory;
    private final EventStreamReader<Object> reader;
    
    private CompressionDemo(String pravegaUri, String registryUri) {
        
        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaUri)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(registryUri)).build();
        client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        scope = "scope";
        stream = "compression";
        groupId = NameUtils.getScopedStreamName(scope, stream);
        schema1 = AvroSchema.ofRecord(SCHEMA1);
        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        initialize();
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .decoder(MY_CODEC.getName(), MY_CODEC)
                                                             .registryClient(client)
                                                             .build();

        Serializer<Object> readerDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        String readerGroup = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(readerGroup,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        reader = clientFactory.createReader("r2", readerGroup, readerDeserializer, ReaderConfig.builder().build());
    }
    
    public static void main(String[] args) {
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

        CompressionDemo demo = new CompressionDemo(pravegaUri, registryUri);
        while (true) {
            System.out.println("1. Gzip");
            System.out.println("2. Snappy");
            System.out.println("3. None");
            System.out.println("4. Custom");
            System.out.println("5. Print all codecTypes");
            System.out.println("6. Read all");
            System.out.println("7. exit");
            Scanner in = new Scanner(System.in);
            int s;
            try {
                s = Integer.parseInt(in.nextLine());

                int size;
                switch (s) {
                    case 1:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());

                        demo.writeGzip(generateBigString(size));
                        break;
                    case 2:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.writeSnappy(generateBigString(size));
                        break;
                    case 3:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.withoutCompression(generateBigString(size));
                        break;
                    case 4:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.writeCustom(generateBigString(size));
                        break;
                    case 5:
                        demo.printAllCodecTypes();
                        break;
                    case 6:
                        demo.readMessages();
                        break;
                    case 7:
                        System.exit(0);
                        break;
                    default:
                        System.err.println("invalid choice");
                        break;
                }
            } catch (NumberFormatException e) {
                System.out.println("invalid choice");
                continue;
            }
        }
    }
    
    private void initialize() {
        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, 
                    new GroupProperties(serializationFormat, Compatibility.backward(), true));
        }
    }

    private void printAllCodecTypes() {
        List<CodecType> list = client.getCodecTypes(groupId);
        System.out.println(list.stream().map(CodecType::getName).collect(Collectors.toList()));
    }
    
    private void writeGzip(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .encoder(Codecs.GzipCompressor.getCodec())
                                                            .registryClient(client)
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }
    
    private void writeSnappy(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .encoder(Codecs.SnappyCompressor.getCodec())
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .registryClient(client)
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }
    
    private void writeCustom(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .encoder(MY_CODEC)
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .registryClient(client)
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());

        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }

    private void withoutCompression(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .registryClient(client)
                                                            .build();
        
        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();

    }

    private static String generateBigString(int sizeInKb) {
        byte[] array = new byte[1024 * sizeInKb];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }

    private void readMessages() {
        EventRead<Object> event = reader.readNextEvent(1000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            Object e = event.getEvent();
            System.out.println("event read = " + e.toString());
            event = reader.readNextEvent(1000);
        }
    }
}

