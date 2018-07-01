package com.hadoop.learn.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RunJob {

    static Configuration conf;

    public static void main (String[] args) {
        // 加载配置文件
        try {
            conf = new Configuration();
            // 设置测试用配置
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("yarn.resourcemanager.hostname", "localhost");
            // 实例化hdfs
            FileSystem fs = FileSystem.get(conf);
            // 获取job实例
            Job job = Job.getInstance(conf);

            job.setJarByClass(RunJob.class);
            job.setMapperClass(MyMapper.class);
            job.setPartitionerClass(MyPartitioner.class);
            job.setSortComparatorClass(MySort.class);
            job.setGroupingComparatorClass(MyGroup.class);
            job.setReducerClass(MyReducer.class);
            job.setMapOutputKeyClass(Weather.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(3);
            // 添加数据输入路径
            Path input = new Path("/weather/input/data");
            if (!fs.exists(input)) {
                System.out.println("输入文件不存在！");
                System.exit(1);
            }
            FileInputFormat.addInputPath(job, input);

            Path output = new Path("/weather/output");
            // 保证输出路径不存在
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            // 设置数据输出路劲
            FileOutputFormat.setOutputPath(job, output);
            boolean res = job.waitForCompletion(true);
            if(res){
                System.out.println("job 成功执行");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

