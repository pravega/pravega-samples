package io.pravega.connectors.nytaxi.common;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@Builder
@EqualsAndHashCode
public final class ZoneLookup implements Serializable {

    private int locationId;
    private String borough;
    private String zone;
    private String serviceZone;

    public static ZoneLookup parse(String line) {
        String[] tokens = line.split(",");

        if (tokens.length != 4) {
            throw new RuntimeException("Invalid Zone Lookup record: " + line);
        }

        ZoneLookupBuilder builder = new ZoneLookupBuilder();

        int offset = 1;

        for (String data: tokens) {

            if ( offset == 1 ) {
                builder.locationId(Integer.parseInt(data));
            } else if ( offset == 2 ) {
                builder.borough(data);
            } else if ( offset == 3 ) {
                builder.zone(data);
            } else if ( offset == 4 ) {
                builder.serviceZone(data);
            }

            offset++;
        }

        return builder.build();

    }

}
