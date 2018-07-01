package com.hadoop.learn.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapred.jar", "/Users/zhengyifan/app/project/bigdata-learn/hadoop/hadoop.jar");
        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.getName());
        job.setReducerClass(WCReduce.class);
        Path path = new Path("/tmp/wc");
        FileInputFormat.addInputPath(job, path);

        Path outpath = new Path("/tmp/out_wc");
        // 保证文件不存在
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);
        boolean res = job.waitForCompletion(true);
        if (res == true) {
            System.out.println("执行成功！");
        }else{
            System.out.println("执行失败！");
        }
    }

}
