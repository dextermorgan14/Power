package com.ravindra.power;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


    /**
     * Created by ravindra on 1/10/16.
     */
    public class DateDriver {

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage power <input path>  <output path>");
                System.exit(-1);
            }
            Job job = new Job();
            job.setJarByClass(DateDriver.class);
            job.setJobName("Rock the power");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(DateMapper.class);
            job.setReducerClass(DateReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }


