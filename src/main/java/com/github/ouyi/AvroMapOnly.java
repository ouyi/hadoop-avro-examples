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
 * A simple map-only job where the input and output share the same schema
 */
public class AvroMapOnly extends Configured implements Tool {

    static class AvroIdFilterMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        private static final int MAGIC_NUMBER = 3;

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            GenericRecord mapIn = key.datum();
            int id = (Integer) mapIn.get("id");

            // Filter by one field
            if (id != MAGIC_NUMBER) {

                // map output uses the same schema as map input
                GenericRecord mapOut = new GenericData.Record(mapIn.getSchema());
                mapOut.put("id", mapIn.get("id"));
                mapOut.put("name", mapIn.get("name"));
                mapOut.put("count", mapIn.get("count"));

                // Output can also use the generated type, then the map output key type shall be AvroKey<Imps> and the key object can be created as follows:
                // Imps mapOut = Imps.newBuilder().setId(id).setName((CharSequence) mapIn.get("name")).setCount((Integer) mapIn.get("count")).build();
                context.write(new AvroKey(mapOut), NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroMapOnly(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Running with args: " + Arrays.deepToString(args));
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Schema schema = new Schema.Parser().parse(new File(args[2]));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // Required due to Error: java.io.IOException: AvroKeyOutputFormat requires an output schema. Use AvroJob.setOutputKeySchema()
        AvroJob.setOutputKeySchema(job, schema);

        job.setMapperClass(AvroIdFilterMapper.class);

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
