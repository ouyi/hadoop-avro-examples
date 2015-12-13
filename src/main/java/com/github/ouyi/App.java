package com.github.ouyi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Hello world!
 */
public class App extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    System.out.println("Called main with args: " + Arrays.deepToString(args));
    int exitCode = ToolRunner.run(new App(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
