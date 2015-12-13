package com.github.ouyi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * Hello world!
 */
public class App extends Configured implements Tool {

  static class MultipleOutputsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      mos.write(NullWritable.get(), value, value.toString().split("\t")[0]);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

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
    job.setMapperClass(MultipleOutputsMapper.class);
    job.setNumReduceTasks(0);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
