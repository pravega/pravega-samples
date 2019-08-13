package io.pravega.example.mqtt;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Wrapper class that holds raw data and its corresponding annotation info
 */
public class DataPacket implements Serializable {

    private String carId;

    private long timestamp;

    private byte[] payload;

    private byte[] annotation;

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getAnnotation() {
        return annotation;
    }

    public void setAnnotation(byte[] annotation) {
        this.annotation = annotation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DataPacket{" +
                "carId='" + carId + '\'' +
                ", timestamp=" + timestamp +
                ", payload=" + Arrays.toString(payload) +
                ", annotation=" + Arrays.toString(annotation) +
                '}';
    }
}
