package io.pravega.example.mqtt;

import com.google.common.base.Preconditions;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

public class MqttConnectionBuilder {

    private String brokerUri;
    private String topic;
    private String clientId;
    private String userName;
    private String password;
    private MqttCallback mqttCallback;
    private MqttClientPersistence persistence;

    public MqttConnectionBuilder brokerUri(String brokerUri) {
        this.brokerUri = brokerUri;
        return this;
    }

    public MqttConnectionBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public MqttConnectionBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public MqttConnectionBuilder userName(String userName) {
        this.userName = userName;
        return this;
    }

    public MqttConnectionBuilder password(String password) {
        this.password = password;
        return this;
    }

    public MqttConnectionBuilder bridge(MqttCallback mqttCallback) {
        this.mqttCallback = mqttCallback;
        return this;
    }

    public MqttConnectionBuilder persistence(MqttClientPersistence persistence) {
        this.persistence = persistence;
        return this;
    }

    public MqttClient connect() throws MqttException {

        Preconditions.checkNotNull(brokerUri, "Missing MQTT broker information");
        Preconditions.checkNotNull(topic, "Missing MQTT topic information");
        Preconditions.checkNotNull(clientId, "Missing MQTT clientId");
        Preconditions.checkNotNull(userName, "Missing MQTT userName");
        Preconditions.checkNotNull(password, "Missing MQTT password");
        Preconditions.checkNotNull(mqttCallback, "Missing MQTT callback handler");


        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName(userName);
        mqttConnectOptions.setPassword(password.toCharArray());

        MqttClient mqttClient;
        if (persistence != null) {
            mqttClient = new MqttClient(brokerUri, clientId, persistence);
        } else {
            mqttClient = new MqttClient(brokerUri, clientId);
        }

        mqttClient.setCallback(mqttCallback);
        mqttClient.connect(mqttConnectOptions);
        mqttClient.subscribe(topic);

        return mqttClient;
    }
}
