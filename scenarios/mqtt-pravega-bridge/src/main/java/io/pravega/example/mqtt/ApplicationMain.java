package io.pravega.example.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ApplicationMain {

    private static Logger log = LoggerFactory.getLogger( ApplicationMain.class );

    public static void main(String ... args) {

        if (args.length != 1) {
            log.error("Missing required arguments. Usage: java io.pravega.example.mqtt.ApplicationMain <CONF_DIR_PATH>");
            return;
        }

        String confDir = args[0];
        log.info("loading configurations from {}", confDir);

        final CountDownLatch latch = new CountDownLatch(1);

        try {
            ApplicationArguments applicationArguments = new ApplicationArguments(confDir);
            MqttListener listener = new MqttListener(applicationArguments.getPravegaArgs());

            MqttConnectionBuilder builder = new MqttConnectionBuilder();
            builder.brokerUri(applicationArguments.getMqttArgs().brokerUri);
            builder.topic(applicationArguments.getMqttArgs().topic);
            builder.clientId(applicationArguments.getMqttArgs().clientId);
            builder.userName(applicationArguments.getMqttArgs().userName);
            builder.password(applicationArguments.getMqttArgs().password);
            builder.bridge(listener);

            MqttClient mqttClient = builder.connect();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Going to close the application");
                if (mqttClient != null) {
                    try {
                        mqttClient.close();
                    } catch (MqttException e) {
                        log.error("Exception Occurred while closing MQTT client", e);
                    }
                }
                latch.countDown();
            }));
        } catch (Exception e) {
            log.error("Exception Occurred", e);
        }

    }
}
