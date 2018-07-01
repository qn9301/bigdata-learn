package com.hadoop.learn.fof.job1;

import com.hadoop.learn.fof.User;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<User, IntWritable, User, IntWritable>{

    @Override
    protected void reduce(User key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int relation = 0;
        for (IntWritable val : values) {
            if (val.get() == 0) {
                break;
            }
            relation += val.get();
        }
        if (relation > 0) {
            context.write(key, new IntWritable(relation));
        }
    }
}
