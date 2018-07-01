package com.hadoop.learn.fof.job2;

import com.hadoop.learn.fof.User2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<User2, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(User2 key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int temp_relation = 0;
        for (IntWritable i : values) {
            if (temp_relation == 0) {
                context.write(new Text(key.format()), i);
                temp_relation = i.get();
            }
            else if (temp_relation == i.get())
            {
                context.write(new Text(key.format()), i);
            }
            else
            {
                break;
            }
        }
    }
}
