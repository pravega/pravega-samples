/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.example.hadoop;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A special PravegaOutputFormat to write events to Pravega stream in sequence:
 *   For a map/reduce job, an output stream will be created with segments specified (default value is 3);
 *   for each reducer (or mapper if no reducer) task it will receive a record-writer with its task id embedded,
 *   when events are being written into stream, original event key will be replaced with the task id,
 *   hence all the events from one mapper/reducer task will go to a particular segment, hence the sequence is kept.
 *
 * The "sequence" here means:
 * All the keys in a segment are in order;
 * Segments are in order too by segment id - all the keys in segment with lower id are less than all the keys in
 * segments with greater ids.
 *
 */
public class PravegaSequenceOutputFormat<V> extends PravegaOutputFormat<V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaSequenceOutputFormat.class);

    @Override
    public RecordWriter<String, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return getRecordWriter(context, String.valueOf(context.getTaskAttemptID().getTaskID().getId()));
    }

}
