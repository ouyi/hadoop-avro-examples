package com.github.ouyi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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
 * A simple map-only job where the input and output have different schemas
 */
public class AvroMapOnlyDifferentSchema extends Configured implements Tool {

    private static final String schemaKey = "outputSchema";

    static class AvroIdFilterMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        private static final int MAGIC_NUMBER = 3;
        private Schema outputSchema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String schemaStr = context.getConfiguration().get(schemaKey);
            outputSchema = new Schema.Parser().parse(schemaStr);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            GenericRecord mapIn = key.datum();
            int id = (Integer) mapIn.get("id");

            // Filter by one field
            if (id != MAGIC_NUMBER) {

                GenericRecord mapOut = new GenericData.Record(outputSchema);
                mapOut.put("id", mapIn.get("id"));
                mapOut.put("name", mapIn.get("name"));
                GenericRecord address = new GenericData.Record((GenericData.Record) mapIn.get("address"), true);
                mapOut.put("address", address);
                context.write(new AvroKey(mapOut), NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroMapOnlyDifferentSchema(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Running with args: " + Arrays.deepToString(args));

        Schema outputSchema = new Schema.Parser().parse(new File(args[2]));
        // Pass this to the mapper
        getConf().set(schemaKey, outputSchema.toString());
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // Required due to Error: java.io.IOException: AvroKeyOutputFormat requires an output outputSchema. Use AvroJob.setOutputKeySchema()
        AvroJob.setOutputKeySchema(job, outputSchema);

        job.setMapperClass(AvroIdFilterMapper.class);

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
