/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.examples.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
 * This class is copied from apache/hadoop and modified by removing
 * unsupported commands
 *
 * A description of an example program based on its class and a
 * human-readable description.
 */
public class ExampleDriver {

    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in pravega.");
            pgd.addClass("wordmean", WordMean.class,
                    "A map/reduce program that counts the average length of the words in pravega.");
            pgd.addClass("wordmedian", WordMedian.class,
                    "A map/reduce program that counts the median length of the words in pravega.");
            pgd.addClass("randomwriter", RandomWriter.class,
                    "A map/reduce program that writes random data to pravega.");
            pgd.addClass("randomtextwriter", RandomTextWriter.class,
                    "A map/reduce program that writes random textual data to pravega.");
            exitCode = pgd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
