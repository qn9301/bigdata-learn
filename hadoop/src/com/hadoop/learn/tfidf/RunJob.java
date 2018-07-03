package com.hadoop.learn.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class RunJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("mapred.jar", "/Users/zhengyifan/git/bigdata-learn/hadoop/out/artifacts/hadoop_jar2/hadoop.jar");

            FileSystem fs = FileSystem.newInstance(conf);
            Job1 job1 = new Job1();
            Job2 job2 = new Job2();
            Job3 job3 = new Job3();
            job3.run(conf, fs);
            if (job1.run(conf, fs)) {
                if (job2.run(conf, fs)) {
                    job3.run(conf, fs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("执行失败！");
        }

    }
}
