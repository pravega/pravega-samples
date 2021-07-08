# MQTT to Pravega Bridge

This sample application reads events from MQTT and writes them to a Pravega stream.

## Usage

- Install Mosquitto MQTT broker and clients.
  ```
  sudo apt-get install mosquitto mosquitto-clients
  ```

- If not automatically started, start Mosquitto broker.
  ```
  mosquitto
  ```

- Edit the file src/main/dist/conf/bridge.properties
  to specify your Pravega controller URI (controllerUri) as
  `tcp://HOST_IP:9090`.

- Run the application:
  ```
  ../../gradlew run
  ```

- Alternatively, you may run in IntelliJ.
  Run the class ApplicationMain with the following parameters:
  ```
  scenarios/mqtt-pravega-bridge/src/main/dist/conf
  ```

- Publish a sample MQTT message.
  Note that the topic must be formatted as "topic/car_id" as shown below.
  ```
  mosquitto_pub -t center/0001 -m "12,34,56.78"
  ```

- You should see the following application output:
  ```
  [MQTT Call: CanDataReader] io.pravega.example.mqtt.MqttListener: Writing Data Packet: CarID: 0001 Timestamp: 1551671403118 Payload: [B@2813d92f annotation: null
  ```
