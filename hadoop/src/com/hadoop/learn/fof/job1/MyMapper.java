package com.hadoop.learn.fof.job1;

import com.hadoop.learn.fof.User;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, User, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 分隔字符串
        String[] line = value.toString().split("\t");
        for (int i=0; i<line.length; i++) {
            int relation = 1;
            if (i == 0) {
                relation = 0;
            }
            for (int j=i+1; j<line.length;j++) {
                User user = new User(line[i], line[j], relation);
                context.write(user, new IntWritable(relation));
            }
        }
    }
}
