/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.example.hadoop.wordcount;

import io.pravega.example.hadoop.terasort.TeraGen;
import io.pravega.example.hadoop.terasort.TeraStreamValidate;
import io.pravega.example.hadoop.terasort.TeraSort;
import org.apache.hadoop.util.ProgramDriver;

/**
 * This class is copied from apache/hadoop and modified by removing
 * unsupported commands
 *
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-examples
 * /src/main/java/org/apache/hadoop/examples/ExampleDriver.java
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
            pgd.addClass("teragen", TeraGen.class,
                    "A map/reduce program that generate the official GraySort input data set to pravega.");
            pgd.addClass("terasort", TeraSort.class,
                    "A map/reduce program that sorts events from one pravega stream and write the sorted " +
                            "events into one or more streams in a globally sorted manner.");
            pgd.addClass("terastreamvalidate", TeraStreamValidate.class,
                    "A map/reduce program that reads events from sorted streams and write into hdfs files " +
                            "for validation purpose");
            exitCode = pgd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
