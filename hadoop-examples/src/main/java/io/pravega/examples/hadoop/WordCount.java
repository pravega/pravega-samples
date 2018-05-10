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

import com.google.common.base.Charsets;


/**
 * This class is copied from apache/hadoop and modified by adding logic to
 * support PravegaInputFormat
 *
 */
public class WordCount {

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
            System.err.println("Usage: wordcount <dummy_hdfs> <uri> <scope> <stream> <out>");
            System.exit(2);
        }

        conf.setStrings("pravega.uri", otherArgs[1]);
        conf.setStrings("pravega.scope", otherArgs[2]);
        conf.setStrings("pravega.stream", otherArgs[3]);
        conf.setStrings("pravega.deserializer", TextSerializer.class.getName());

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(PravegaInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputpath = new Path(otherArgs[4]);
        FileOutputFormat.setOutputPath(job, outputpath);

        boolean result = job.waitForCompletion(true);
        readAndPrint(outputpath, conf);
        System.exit(result ? 0 : 1);
    }
}
