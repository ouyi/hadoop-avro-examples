package com.github.ouyi;

import com.github.ouyi.avro.Imps;
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
 * Hello world!
 */
public class AvroMapOnly extends Configured implements Tool {

    static class AvroIdentityMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            GenericRecord record = key.datum();
            int id = (Integer) record.get("id");
            if (id != 3) {
                GenericRecord r = new GenericData.Record(record.getSchema());
                r.put("id", record.get("id"));
                r.put("name", record.get("name"));
                r.put("count", record.get("count"));
//                Imps r = Imps.newBuilder().setId((Integer) record.get("id")).setName((CharSequence) record.get("name")).setCount((Integer) record.get("count")).build();
                context.write(new AvroKey(r), NullWritable.get());
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
        AvroJob.setInputKeySchema(job, schema);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, schema);
        job.setMapperClass(AvroIdentityMapper.class);

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
