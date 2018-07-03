package com.hadoop.learn.tfidf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Job3 {

    public boolean run (Configuration conf, FileSystem fs) {
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(Job3.class);
            job.setJobName("job3");
            job.setMapperClass(Job3Mapper.class);
            job.setReducerClass(Job3Reducer.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            Path input = new Path("/tfidf/output1");
            Path output = new Path("/tfidf/output3");

            // 存缓存
            Path cachefile1 = new Path("/tfidf/output1/part-r-00003");
            Path cachefile2 = new Path("/tfidf/output2/part-r-00000");

            job.addCacheFile(cachefile1.toUri());
            job.addCacheFile(cachefile2.toUri());

            if (!fs.exists(input)) {
                System.out.println("job3 输入文件不存在！");
                return false;
            }

            if (fs.exists(output)) {
                fs.delete(output, true);
            }

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            boolean res = job.waitForCompletion(true);
            if (res) {
                System.out.println("job3 执行完成！");
                return true;
            } else {
                System.out.println("job3 执行失败！");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

class Job3Mapper extends Mapper<Text, Text, Text, Text> {

    public static Map<String, Integer> countMap = null;;

    public static Map<String, Integer> wordMap = null;

    /**
     * 设置缓存到内存
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) {
        try{
            System.out.println("setup");
            URI[] uri = context.getCacheFiles();
            if (uri != null) {
                System.out.println("uri length: " + uri.length);
                for (int i=0; i < uri.length; i++) {
                    if (uri[i].getPath().endsWith("part-r-00003")) {//微博总数
                        Path path =new Path(uri[i].getPath());
//						FileSystem fs =FileSystem.get(context.getConfiguration());
//						fs.open(path);
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                        String line = br.readLine();
                        if (line.startsWith("count")) {
                            String[] ls = line.split("\t");
                            countMap = new HashMap<String, Integer>();
                            countMap.put(ls[0], Integer.parseInt(ls[1].trim()));
                        }
                        br.close();
                    } else if (uri[i].getPath().endsWith("part-r-00000")) {//词条的DF
                        wordMap = new HashMap<String, Integer>();
                        Path path =new Path(uri[i].getPath());
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] ls = line.split("\t");
                            wordMap.put(ls[0], Integer.parseInt(ls[1].trim()));
                        }
                        br.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 套用公式
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) {
        try {
            FileSplit fs = (FileSplit) context.getInputSplit();

            if (!fs.getPath().getName().contains("part-r-00003")) {
                String keyStr = key.toString();
                String[] arrStr = keyStr.split("_");
                Integer val = Integer.parseInt(value.toString());
                if (wordMap.containsKey(arrStr[0])) {
                    String res = calculate(countMap.get("count"), val, wordMap.get(arrStr[0]));
                    System.out.println(arrStr[1] + "---" + arrStr[0] + ":" + res);
                    context.write(new Text(arrStr[1]), new Text(arrStr[0] + ":" + res));
                } else {
                    System.out.println(arrStr[0] + " 不存在！");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * D 文件总数
     * tf
     * J|{j:t《d}|
     * @param D
     * @param tf
     * @param J
     * @return
     */
    private String calculate(Integer D, Integer tf, Integer J) {
        double res = tf * Math.log(D / J);
        NumberFormat nf =NumberFormat.getInstance();
        nf.setMaximumFractionDigits(5);
        return nf.format(res);
    }
}

class Job3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer sb =new StringBuffer();
        for( Text i : values ){
            sb.append(i.toString()+"\t");
        }
        context.write(key, new Text(sb.toString()));
    }
}