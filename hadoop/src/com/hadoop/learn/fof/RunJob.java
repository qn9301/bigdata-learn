package com.hadoop.learn.fof;

import com.hadoop.learn.fof.job1.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

    static Configuration conf;

    static FileSystem fs;

    public static void main(String[] args) {
        try {
            conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("yarn.resourcemanager.hostname", "localhost");
            fs = FileSystem.newInstance(conf);
            if (job1()) {
                System.out.println("job1执行完成！");
                job2();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean job1() {
        try
        {
            Job job = Job.getInstance(conf);
            job.setJarByClass(com.hadoop.learn.fof.RunJob.class);
            job.setJobName("jobOne");
            job.setMapperClass(MyMapper.class);
            job.setCombinerClass(MyCombiner.class);
            job.setSortComparatorClass(MySort.class);
            job.setGroupingComparatorClass(MyGroup.class);
            job.setReducerClass(MyReducer.class);
            job.setMapOutputKeyClass(User.class);
            job.setMapOutputValueClass(IntWritable.class);

            Path input = new Path("/fof/input/data");
            if (!fs.exists(input)) {
                System.out.println("job1的输入文件不存在！");
                return false;
            }
            Path output = new Path("/fof/output1");
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileInputFormat.addInputPath(job, input);

            FileOutputFormat.setOutputPath(job, output);

            boolean res = job.waitForCompletion(true);

            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void job2() {
        try
        {
            Job job = Job.getInstance(conf);
            job.setJarByClass(com.hadoop.learn.fof.RunJob.class);
            job.setJobName("jobTwo");
            job.setMapperClass(com.hadoop.learn.fof.job2.MyMapper.class);
            job.setGroupingComparatorClass(com.hadoop.learn.fof.job2.MyGroup.class);
            job.setReducerClass(com.hadoop.learn.fof.job2.MyReducer.class);
            job.setMapOutputKeyClass(User2.class);
            job.setMapOutputValueClass(IntWritable.class);

            Path input = new Path("/fof/output1");
            if (!fs.exists(input)) {
                System.out.println("job2的输入文件不存在！");
            }
            Path output = new Path("/fof/output2");
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileInputFormat.addInputPath(job, input);

            FileOutputFormat.setOutputPath(job, output);

            boolean res = job.waitForCompletion(true);
            if (res)
            {
                System.out.println("job2执行完成！");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

