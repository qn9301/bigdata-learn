package com.hadoop.learn.fof.job2;

import com.hadoop.learn.fof.User2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, User2, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] list = line.split("\t");
        User2 user1 = new User2();
        User2 user2 = new User2();
        int relation = Integer.parseInt(list[1]);
        String[] people = list[0].split("-");
        user2.setMe(people[0]);
        user2.setFriend(people[1]);
        user2.setRelation(relation);
        user1.setMe(people[1]);
        user1.setFriend(people[0]);
        user1.setRelation(relation);
        context.write(user1, new IntWritable(relation));
        context.write(user2, new IntWritable(relation));
    }
}
