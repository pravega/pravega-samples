package io.pravega.example.mqtt;

import io.pravega.client.stream.EventStreamWriter;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * MQTT Listener call back handler that listens to specified MQTT topic and fetches the posted data
 */

public class MqttListener implements MqttCallback {

    private static Logger log = LoggerFactory.getLogger( MqttListener.class );

    private final EventStreamWriter<DataPacket> writer;

    public MqttListener(ApplicationArguments.PravegaArgs pravegaArgs) {
        writer = PravegaHelper.getStreamWriter(pravegaArgs);
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.debug("Received connection lost message. Reason: {}", cause);
        writer.close();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.debug("Received new message from the topic: {}", topic);

        String carId = topic.split("/")[1];
        DataPacket packet = new DataPacket();
        packet.setTimestamp(System.currentTimeMillis());
        packet.setCarId(carId);
        packet.setPayload(message.getPayload());

        log.info("Writing Data Packet: {}", packet);

        CompletableFuture<Void> future = writer.writeEvent(carId, packet);
        future.get();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

}
