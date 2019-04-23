/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.spark.wordcount;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaConfig;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import io.pravega.example.hadoop.wordcount.TextSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;


public final class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: WordCount <url> <scope> <stream>");
            System.exit(2);
        }

        conf.setStrings(PravegaConfig.INPUT_URI_STRING, remainingArgs[0]);
        conf.setStrings(PravegaConfig.INPUT_SCOPE_NAME, remainingArgs[1]);
        conf.setStrings(PravegaConfig.INPUT_STREAM_NAME, remainingArgs[2]);
        conf.setStrings(PravegaConfig.INPUT_DESERIALIZER, TextSerializer.class.getName());

        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(SparkContext.getOrCreate(sparkConf));

        JavaPairRDD<EventKey, Text> lines = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, Text.class);
        JavaRDD<String> words = lines.map(x -> x._2).flatMap(s -> Arrays.asList(SPACE.split(s.toString())).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        System.out.println("RESULT :" + counts.collect());
    }
}
