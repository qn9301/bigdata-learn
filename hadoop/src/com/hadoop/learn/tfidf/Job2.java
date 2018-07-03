package com.hadoop.learn.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job2 {

    public boolean run (Configuration conf, FileSystem fs) {
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(Job2.class);
            job.setJobName("job2");
            job.setMapperClass(Job2Mapper.class);
            job.setReducerClass(Job2Reducer.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            Path input = new Path("/tfidf/output1");
            Path output = new Path("/tfidf/output2");

            if (!fs.exists(input)) {
                System.out.println("job2 输入文件不存在！");
                return false;
            }

            if (fs.exists(output)) {
                fs.delete(output, true);
            }

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            boolean res = job.waitForCompletion(true);
            if (res) {
                System.out.println("job2 执行完成！");
                return true;
            } else {
                System.out.println("job2 执行失败！");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

class Job2Mapper extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        if (!key.equals(new Text("count"))) {
            String str = key.toString();
            String[] arr = str.split("_");
            if (arr.length > 1) {
                String goods = arr[0];
                context.write(new Text(goods), new IntWritable(1));
            }
        }
    }
}

class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}