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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.DataType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.TimeZone;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

public class sender {

    private static final String DEFAULT_SCOPE = "examples";
    private static final String DEFAULT_STREAM = "userBehavior";
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

        DataType dataType =
                ROW(
                        FIELD("user_id", STRING()),
                        FIELD("item_id", STRING()),
                        FIELD("category_id", STRING()),
                        FIELD("behavior", STRING()),
                        FIELD("ts", TIMESTAMP(3))).notNull();
        Schema avroSchema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, AvroSchema.ofRecord(avroSchema));

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
                    ts += tz.getOffset(ts);
                    GenericRecord record = new GenericData.Record(avroSchema);
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

    private static boolean schemaGroupExist(SchemaRegistryClient client, String groupId) {
        try {
            client.getGroupProperties(groupId);
        } catch (RegistryExceptions.ResourceNotFoundException e) {
            return false;
        }
        return true;
    }
}
