/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Dell Inc. 2018 by removing unnecessary classes
 */

package io.pravega.examples.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
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
