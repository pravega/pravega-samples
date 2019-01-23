/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.pravega.connectors.nytaxi;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public class ApplicationMain {

    public static void main(String ... args) {

        StringBuilder usage = new StringBuilder();
        usage.append("\n----------------------------------------------------------------------------------------------------------------------------------\n");
        usage.append("Uasge: java io.pravega.connectors.nytaxi.ApplicationMain --runApp <Prepare|PopularDestinationQuery|PopularTaxiVendor|MaxTravellers> \n");
        usage.append("Additional optional parameters: ");
        usage.append("--scope <scope-name> --stream <stream-name> --controllerUri <controller-uri> --create-stream <true|false> \n");
        usage.append("----------------------------------------------------------------------------------------------------------------------------------");

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        String type;
        try {
            type = params.getRequired("runApp");
        } catch (Exception e) {
            log.error(usage.toString(), e);
            return;
        }

        AbstractHandler handler = null;

        if (type.equals("Prepare")) {
            handler = new PrepareMain(args);
        } else if (type.equals("PopularDestinationQuery")) {
            handler = new PopularDestinationQuery(args);
        } else if (type.equals("PopularTaxiVendor")) {
            handler = new PopularTaxiVendor(args);
        } else if (type.equals("MaxTravellers")) {
            handler = new MaxTravellersPerDestination(args);
        } else {
            log.error(usage.toString());
        }
        handler.handleRequest();
    }
}
