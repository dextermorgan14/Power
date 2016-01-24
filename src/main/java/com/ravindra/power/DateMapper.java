package com.ravindra.power;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ravindra on 1/11/16.
 */
public class DateMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
    {
    String line = value.toString().replaceAll("\\s+", " ").trim();
        String date;
        int l = line.length();
        if(l >1) {
            String[] s = line.split(" ");
            date = s[0];
            if(s.length>4 && date.contains("/13/")) {
                context.write(new Text(date), new Text(s[1]+" "+s[2]+" "+s[3]+" "+s[4]+" "+s[5]+" "+s[6]));

            }
        }

}



}
