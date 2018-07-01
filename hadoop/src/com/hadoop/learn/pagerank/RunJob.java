package com.hadoop.learn.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunJob {

    public static enum Mycounter{
        my
    }
    /**
     * PageRank
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("yarn.resourcemanager.hostname", "localhost");
        conf.setInt(Const.COUNT_KEY, 0);
        FileSystem fs = FileSystem.newInstance(conf);
        int i = 0;
        while (true) {
            i++;
            conf.setInt(Const.COUNT_KEY, i);
            double defaultSum = conf.getDouble(Const.SUM_KEY, 0.0);
            Job job = Job.getInstance(conf);
            job.setJarByClass(RunJob.class);
            job.setJobName(Const.JOB_PREFIX + i);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            // 第一个是1;
            Path input = new Path("/pagerank/data/" + Const.JOB_PREFIX + i);
            Path output = new Path("/pagerank/data/" + Const.JOB_PREFIX + (i+1));
            if (!fs.exists(input)) {
                System.out.println("输入文件不存在！");
                System.exit(1);
            }
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            boolean res = job.waitForCompletion(true);
            if (res) {
                System.out.println("success.");
                long sum= job.getCounters().findCounter(Mycounter.my).getValue();
                System.out.println(sum);
                // counter必须为整数
                double avgd= sum/4000.0;
                if(avgd < Const.THRESHOLD){
                    break;
                }
            }
        }
    }
}
