package com.hadoop.learn.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i : value) {
            count += i.get();
            System.out.println(key.toString() + "---" + count);
        }
        context.write(key, new IntWritable(count));
    }
}
