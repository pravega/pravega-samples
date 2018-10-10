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

package io.pravega.example.hadoop.terasort;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import io.pravega.example.hadoop.wordcount.TextSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *  The class to validate terasort results.
 *  It simply dumps the events of specified Pravega stream into hdfs file specified.
 */
public class TeraStreamValidate extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TeraStreamValidate.class);

  private static void usage() throws IOException {
    System.err.println("Usage: terastreamvalidate [-Dproperty=value] " +
            "<dummy hdfs input> <hdfs output> <pravega uri> <scope> <stream name>");
    System.err.println("TeraSort configurations are:");
    for (TeraSortConfigKeys teraSortConfigKeys : TeraSortConfigKeys.values()) {
      System.err.println(teraSortConfigKeys.toString());
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      usage();
      return 2;
    }
    LOG.info("starting");
    Path inputDir = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    getConf().setStrings("pravega.uri", args[2]);
    getConf().setStrings("pravega.scope", args[3]);
    getConf().setStrings("pravega.stream", args[4]);
    getConf().setStrings("pravega.deserializer", TextSerializer.class.getName());

    getConf().setInt(MRJobConfig.NUM_MAPS, 1);
    Job job = Job.getInstance(getConf());

    TeraInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.setJobName("TeraStreamValidate");
    job.setJarByClass(TeraStreamValidate.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(TeraSortMapper.class);
    job.setNumReduceTasks(1);

    job.setInputFormatClass(PravegaInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    int ret = job.waitForCompletion(true) ? 0 : 1;
    LOG.info("done");
    return ret;
  }

  public static class TeraSortMapper
    extends Mapper<EventKey, Text, Text, Text> {

    public void map(EventKey key, Text value, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(), value);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TeraStreamValidate(), args);
    System.exit(res);
  }

}
