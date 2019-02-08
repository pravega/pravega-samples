/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.hadoop.wordcount;

import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;


/**
 * This class is copied from apache/hadoop and modified by adding logic to
 * support PravegaInputFormat
 *
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-examples
 * /src/main/java/org/apache/hadoop/examples/WordCount.java
 *
 */
public class WordCount {

    private static final Logger log = LoggerFactory.getLogger(WordCount.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object ignored, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Reads the output file
     *
     * @param path The path to find the output file in. Set in main to the output
     *             directory.
     * @throws IOException If it cannot access the output directory, we throw an exception.
     */
    private static void readAndPrint(Path path, Configuration conf)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");

        if (!fs.exists(file))
            throw new IOException("Output not found!");

        BufferedReader br = null;

        try {
            br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
            long count = 0;
            long length = 0;

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 5) {
            System.err.println("Usage: wordcount <dummy_hdfs> <uri> <scope> <stream> <out> <optional start stream cut> <optional end stream cut>");
            System.exit(2);
        }

        String uri = otherArgs[1], scope = otherArgs[2], stream = otherArgs[3], out = otherArgs[4];

        conf.setStrings("input.pravega.uri", uri);
        conf.setStrings("input.pravega.scope", scope);
        conf.setStrings("input.pravega.stream", stream);
        conf.setStrings("input.pravega.deserializer", TextSerializer.class.getName());

        if (otherArgs.length >= 6) {
            conf.setStrings("input.pravega.startpositions", otherArgs[5]);
        }

        String endPos = "";
        if (otherArgs.length >= 7) {
            endPos = otherArgs[6];
        } else {
            endPos = PravegaInputFormat.fetchLatestPosition(uri, scope, stream);
        }
        conf.setStrings("input.pravega.endpositions", endPos);

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(PravegaInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputpath = new Path(out);
        FileOutputFormat.setOutputPath(job, outputpath);

        boolean result = job.waitForCompletion(true);
        readAndPrint(outputpath, conf);

        log.info("End positions of stream cut {}/{}: '{}'\n", scope, stream, endPos);

        System.exit(result ? 0 : 1);
    }
}
