package com.ravindra.power;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ravindra on 1/10/16.
 */
public class PowerReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key,IntWritable values,Context context) throws IOException, InterruptedException
    {


context.write(key, values);


    }



}
