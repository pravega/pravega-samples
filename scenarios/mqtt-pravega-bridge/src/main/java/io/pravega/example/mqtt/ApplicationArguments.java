package io.pravega.example.mqtt;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ApplicationArguments {

    private final PravegaArgs pravegaArgs = new PravegaArgs();
    private final MqttArgs mqttArgs = new MqttArgs();

    public ApplicationArguments(String confDir) throws Exception {
        loadProperties(confDir);
    }

    private void loadProperties(String confDir) throws Exception{
        Properties prop = new Properties();
        try (
                InputStream inputStream = new FileInputStream(confDir + File.separator + "bridge.properties");
             )
        {
            prop.load(inputStream);

            pravegaArgs.controllerUri = prop.getProperty("controllerUri");
            pravegaArgs.scope = prop.getProperty("scope");
            pravegaArgs.stream = prop.getProperty("stream");
            pravegaArgs.targetRate = Integer.parseInt(prop.getProperty("scaling.targetRate"));
            pravegaArgs.scaleFactor = Integer.parseInt(prop.getProperty("scaling.scaleFactor"));
            pravegaArgs.minNumSegments = Integer.parseInt(prop.getProperty("scaling.minNumSegments"));

            mqttArgs.brokerUri = prop.getProperty("brokerUri");
            mqttArgs.topic = prop.getProperty("topic");
            mqttArgs.clientId = prop.getProperty("clientId");
            mqttArgs.userName = prop.getProperty("userName");
            mqttArgs.password = prop.getProperty("password");

            Preconditions.checkNotNull(pravegaArgs.controllerUri, "Pravega Controller URI is missing");
            Preconditions.checkNotNull(pravegaArgs.scope, "Pravega scope is missing");
            Preconditions.checkNotNull(pravegaArgs.stream, "Pravega stream is missing");

            Preconditions.checkNotNull(mqttArgs.brokerUri, "MQTT Broker URI is missing");
            Preconditions.checkNotNull(mqttArgs.topic, "MQTT topic is missing");
            Preconditions.checkNotNull(mqttArgs.clientId, "MQTT clientId is missing");
            Preconditions.checkNotNull(mqttArgs.userName, "MQTT userName is missing");
            Preconditions.checkNotNull(mqttArgs.password, "MQTT password is missing");
        }
    }

    public PravegaArgs getPravegaArgs() {
        return pravegaArgs;
    }

    public MqttArgs getMqttArgs() {
        return mqttArgs;
    }

    public static class PravegaArgs {
        protected String controllerUri;
        protected String scope;
        protected String stream;
        protected int targetRate;
        protected int scaleFactor;
        protected int minNumSegments;
    }

    public static class MqttArgs {
        protected String brokerUri;
        protected String topic;
        protected String clientId;
        protected String userName;
        protected String password;
    }
}
