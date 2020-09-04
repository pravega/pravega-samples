
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.util.TimeZone;

public class sender {

    private static final String DEFAULT_SCOPE = "examples";
    private static final String DEFAULT_STREAM = "testStream";
    private static final String Default_controllerURI = "tcp://127.0.0.1:9090";
    private final static TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");

    public static void main(String[] args) throws IOException {

        // --input
        String arg0 = args[0];
        // input file
        String arg1 = args[1];
        // --speedup
        String arg2 = args[2];
        // speed
        long arg3 = Long.parseLong(args[3]);

        File userBehaviorFile = new File(arg1);
        long speed = arg3 == 0 ? 1000L : arg3;

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(Default_controllerURI))
                .build();
        StreamManager streamManager = null;
        EventStreamClientFactory clientFactory = null;
        EventStreamWriter<String> writer = null;

        try {
            InputStream inputStream = new FileInputStream(userBehaviorFile);
            streamManager = StreamManager.create(clientConfig);
            streamManager.createScope(DEFAULT_SCOPE);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .build();
            streamManager.createStream(DEFAULT_SCOPE, DEFAULT_STREAM, streamConfig);
            clientFactory = EventStreamClientFactory.withScope(DEFAULT_SCOPE, clientConfig);
            writer = clientFactory.createEventWriter(DEFAULT_STREAM, new UTF8StringSerializer(),
                    EventWriterConfig.builder().build());
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                int counter = 0;
                long start = System.nanoTime();
                while (true) {
                    while (reader.ready()) {
                        String line = reader.readLine();
                        String[] splits = line.split(",");
                        long ts = Long.parseLong(splits[4]) * 1000L;
                        Instant instant = Instant.ofEpochMilli(ts + (long) tz.getOffset(ts));
                        String outline = String.format("{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\", \"ts\": \"%s\"}", splits[0], splits[1], splits[2], splits[3], instant.toString());
                        writer.writeEvent(outline);
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
            } finally {
                inputStream.close();
                writer.close();
                clientFactory.close();
                streamManager.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
