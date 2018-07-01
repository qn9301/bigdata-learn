package com.hadoop.learn.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class MyCombiner extends Reducer<Weather, IntWritable, Weather, IntWritable> {
    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
    }
}