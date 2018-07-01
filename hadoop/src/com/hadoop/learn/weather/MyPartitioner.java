package com.hadoop.learn.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner extends HashPartitioner<Weather, IntWritable>{
    @Override
    public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
        return (key.getYear()-1949) % numReduceTasks;
    }
}
