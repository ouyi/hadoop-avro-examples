package com.github.ouyi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Map-only job with Avro input and multiple outputs by input key (the first field in the input value)
 */
public class AvroMoMapOnly extends Configured implements Tool {

    static class AvroMoMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        private AvroMultipleOutputs amos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.amos = new AvroMultipleOutputs(context);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            GenericRecord mapIn = key.datum();
            Schema keySchema = mapIn.getSchema();
            this.amos.write(key, value, keySchema, Schema.create(Schema.Type.NULL), mapIn.get("name").toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.amos.close();
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroMoMapOnly(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        // yarn jar hadoop-avro-examples-1.0-SNAPSHOT.jar com.github.ouyi.AvroMoMapOnly
        // -Dmapreduce.map.maxattempts=2 -Dmapreduce.reduce.maxattempts=2
        // -libjars $(echo lib/* | tr ' ' ',') "/user/input/companies.avro" "output" "input/Company.avsc"

        System.out.println("Running with args: " + Arrays.deepToString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        Schema outputSchema = new Schema.Parser().parse(new File(args[2]));
        AvroJob.setOutputKeySchema(job, outputSchema);

        job.setMapperClass(AvroMoMapper.class);

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;

        /*
        *
Example output:
# hadoop fs -ls output/
16/02/29 21:08:36 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 5 items
-rw-r--r--   1 root supergroup          0 2016-02-29 21:01 output/_SUCCESS
-rw-r--r--   1 root supergroup        412 2016-02-29 21:01 output/aol-m-00000.avro
-rw-r--r--   1 root supergroup        420 2016-02-29 21:01 output/google-m-00000.avro
-rw-r--r--   1 root supergroup        418 2016-02-29 21:01 output/msft-m-00000.avro
-rw-r--r--   1 root supergroup        367 2016-02-29 21:01 output/part-m-00000.avro
        * */
    }
}
