package com.ravindra.power;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ravindra on 1/10/16.
 */
public class PowerMapper extends Mapper <LongWritable,Text,Text,IntWritable>{

    int power;
    String number;
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
    {
        String line= value.toString().replaceAll("\\s+"," ").trim();
        int l = line.length();
        if(l >1) {
            String[] s = line.split(" ");
            number = s[0];
            power=Integer.parseInt(s[2]);
            if(power>2) {
                context.write(new Text(number), new IntWritable(power));

            }
        }

    }


}
