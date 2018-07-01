package com.hadoop.learn.weather;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Weather, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int i = 0;
        for (IntWritable t : values) {
            if (i++ == 2) {
                break;
            }
            String val = key.getYear()+"-"+key.getMonth()+"-"+key.getDay();
            context.write(new Text(val), t);
        }

    }
}