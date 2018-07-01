package com.hadoop.learn.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String val = value.toString();
        String[] list = val.split(" ");
        for (String s : list) {
            System.out.println(s + "\n===" );
            context.write(new Text(s), new IntWritable(1));
        }
    }

}
