package com.ravindra.power;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ravindra on 1/11/16.
 */
public class DateReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key,Text values,Context context) throws IOException, InterruptedException
    {


        context.write(key, values);


    }



}
