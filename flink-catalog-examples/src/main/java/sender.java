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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.codec.Encoder;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractSerializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.TimeZone;

public class sender {

    private static final String DEFAULT_SCOPE = "examples";
    private static final String DEFAULT_STREAM = "testStream";
    private final static TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");

    public static void main(String[] args) throws Exception {

        // --input
        String arg0 = args[0];
        // input file
        String arg1 = args[1];
        // --speedup
        String arg2 = args[2];
        // speed
        long arg3 = Long.parseLong(args[3]);
        // --ctrluri
        String arg4 = args[4];
        // controller uri
        String arg5 = args[5];
        // --scmrgsturi
        String arg6 = args[6];
        // schemaregistry uri
        String arg7 = args[7];

        final URI DEFAULT_CONTROLLER_URI = URI.create(arg5);
        final URI DEFAULT_SCHEMAREGISTRY_URI = URI.create(arg7);

        File userBehaviorFile = new File(arg1);
        long speed = arg3 == 0 ? 1000L : arg3;

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(DEFAULT_CONTROLLER_URI)
                .build();
        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(DEFAULT_SCHEMAREGISTRY_URI)
                .build();

        try (StreamManager streamManager = StreamManager.create(DEFAULT_CONTROLLER_URI)){
            streamManager.createScope(DEFAULT_SCOPE);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .build();
            streamManager.createStream(DEFAULT_SCOPE, DEFAULT_STREAM, streamConfig);
        }

        // add schema registry group
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Json, Compatibility.allowAny(), false);
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
        String schemaString = "{\"type\": \"object\", \"title\": \"user_behavior\", \"properties\": {\"user_id\": " +
                "{\"type\": \"string\"}, \"item_id\": {\"type\": \"string\"}, \"category_id\": " +
                "{\"type\": \"string\"}, \"behavior\": {\"type\": \"string\"}, \"ts\": {\"type\": \"string\", \"format\": \"date-time\"}}}";
        Serializer<JsonNode> serializer = new FlinkJsonSerializer(
                DEFAULT_STREAM,
                SchemaRegistryClientFactory.withNamespace(DEFAULT_SCOPE, schemaRegistryClientConfig),
                JSONSchema.of("", schemaString, JsonNode.class),
                serializerConfig.getEncoder(),
                serializerConfig.isRegisterSchema(),
                serializerConfig.isWriteEncodingHeader());


        try (
                EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(DEFAULT_SCOPE, clientConfig);
                EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(DEFAULT_STREAM, serializer,
                        EventWriterConfig.builder().build());
                InputStream inputStream = new FileInputStream(userBehaviorFile)) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            int counter = 0;
            long start = System.nanoTime();
            ObjectMapper mapper = new ObjectMapper();
            while (true) {
                while (reader.ready()) {
                    String line = reader.readLine();
                    String[] splits = line.split(",");
                    long ts = Long.parseLong(splits[4]) * 1000L;
                    Instant instant = Instant.ofEpochMilli(ts + (long) tz.getOffset(ts));
                    String instantString = instant.toString().substring(0, instant.toString().length() - 1);
                    String outline = String.format("{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\", \"ts\": \"%s\"}", splits[0], splits[1], splits[2], splits[3], instantString);
                    writer.writeEvent(mapper.readTree(outline));
                    System.out.println("Write success!" + outline);
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

    private static boolean schemaGroupExist(SchemaRegistryClient client, String groupId) {
        try {
            client.getGroupProperties(groupId);
        } catch (RegistryExceptions.ResourceNotFoundException e) {
            return false;
        }
        return true;
    }

    private static class FlinkJsonSerializer extends AbstractSerializer<JsonNode> {
        private final ObjectMapper objectMapper;

        public FlinkJsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema schema,
                                   Encoder encoder, boolean registerSchema, boolean encodeHeader) {
            super(groupId, client, schema, encoder, registerSchema, encodeHeader);
            objectMapper = new ObjectMapper();
        }

        @Override
        protected void serialize(JsonNode jsonNode, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
            objectMapper.writeValue(outputStream, jsonNode);
            outputStream.flush();
        }
    }
}
