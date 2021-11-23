/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.TimeZone;

/**
 * This class reads data from input dataset, register the
 * schema to Pravega schema registry service and then send the generated data to Pravega.
 */
public class sender {

    private static final String DEFAULT_FILE_PATH = "/opt/datagen/user_behavior.log";
    private static final long DEFAULT_SPEED = 1000L;
    private static final URI DEFAULT_CONTROLLER_URI = URI.create("tcp://pravega:9090");
    private static final URI DEFAULT_SCHEMAREGISTRY_URI = URI.create("http://schemaregistry:9092");

    private static final String DEFAULT_SCOPE = "examples";
    private static final String DEFAULT_STREAM = "userBehavior";
    private final static TimeZone TIME_ZONE = TimeZone.getTimeZone("Asia/Shanghai");


    /**
     * Parameters
     *     -input Input dataset file path, default:/opt/datagen/user_behavior.log
     *     -speedup Data generating speed, default:1000
     *     -controlleruri Pravega controller uri, default:tcp://pravega:9090
     *     -schemaregistryuri Schema registry service uri, default:http://schemaregistry:9092
     */
    public static void main(String[] args) throws Exception {

        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("sender", options);
            System.exit(1);
        }

        final String filePath = cmd.getOptionValue("input") == null ? DEFAULT_FILE_PATH : cmd.getOptionValue("input");
        File userBehaviorFile = new File(filePath);
        final long speed = cmd.getOptionValue("speedup") == null ?
                DEFAULT_SPEED : Long.parseLong(cmd.getOptionValue("speedup"));
        final URI controllerUri = cmd.getOptionValue("controlleruri") == null ?
                DEFAULT_CONTROLLER_URI : URI.create(cmd.getOptionValue("controlleruri"));
        final URI schemaregistryUri = cmd.getOptionValue("schemaregistryuri") == null ?
                DEFAULT_SCHEMAREGISTRY_URI : URI.create(cmd.getOptionValue("schemaregistryuri"));

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri)
                .build();
        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(schemaregistryUri)
                .build();

        try (StreamManager streamManager = StreamManager.create(controllerUri)){
            streamManager.createScope(DEFAULT_SCOPE);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .build();
            streamManager.createStream(DEFAULT_SCOPE, DEFAULT_STREAM, streamConfig);
        } catch (Exception e) {
            System.out.printf("Meet error when creating stream", e);
            throw e;
        }

        // add schema registry group
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Avro, Compatibility.allowAny(), false);
        try (SchemaRegistryClient client = SchemaRegistryClientFactory.withNamespace(DEFAULT_SCOPE, schemaRegistryClientConfig)) {
            if (!schemaGroupExist(client, DEFAULT_STREAM)) {
                client.addGroup(DEFAULT_STREAM, groupProperties);
            }
        } catch (Exception e) {
            System.out.printf("Meet error when adding schema registry group", e);
            throw e;
        }

        // register schema to schema registry
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(DEFAULT_SCOPE)
                .groupId(DEFAULT_STREAM)
                .registerSchema(true)
                .build();

        Schema userBehavior = SchemaBuilder
                .record("UserBehavior")
                .fields()
                .name("user_id").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("item_id").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("category_id").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("behavior").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("ts").type(LogicalTypes.timestampMillis().
                        addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .endRecord();
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, AvroSchema.ofRecord(userBehavior));

        try (
                EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(DEFAULT_SCOPE, clientConfig);
                EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(DEFAULT_STREAM, serializer,
                        EventWriterConfig.builder().build());
                InputStream inputStream = new FileInputStream(userBehaviorFile)) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            int counter = 0;
            long start = System.nanoTime();
            while (true) {
                while (reader.ready()) {
                    String line = reader.readLine();
                    String[] splits = line.split(",");
                    long ts = Long.parseLong(splits[4]) * 1000L;
                    ts += TIME_ZONE.getOffset(ts);
                    GenericRecord record = new GenericData.Record(userBehavior);
                    record.put(0, splits[0]);
                    record.put(1, splits[1]);
                    record.put(2, splits[2]);
                    record.put(3, splits[3]);
                    record.put(4, ts);
                    writer.writeEvent(record);
                    System.out.println("Write success!" + record);
                    ++counter;
                    if ((long) counter >= speed) {
                        long end = System.nanoTime();
                        for (long diff = end - start; diff < 1000000000L; diff = end - start) {
                            Thread.sleep(1L);
                            end = System.nanoTime();
                        }
                        start = end;
                        counter = 0;
                    }
                }
                reader.close();
                break;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("input", true, "The input file path of dataset.");
        options.addOption("speedup", true, "The data generating speed.");
        options.addOption("controlleruri", true, "The Pravega controller uri");
        options.addOption("schemaregistryuri", true, "The schema-registry service uri.");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

    private static boolean schemaGroupExist(SchemaRegistryClient client, String groupId) {
        try {
            client.getGroupProperties(groupId);
        } catch (RegistryExceptions.ResourceNotFoundException e) {
            return false;
        }
        return true;
    }
}
