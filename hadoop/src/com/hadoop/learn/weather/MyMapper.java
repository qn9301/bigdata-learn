package com.hadoop.learn.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Calendar c = Calendar.getInstance();

        String line = value.toString();
        String[] list = StringUtils.split(line, '\t');
        if (list.length == 2) {
            String arg = list[0];
            String temp = list[1];
            Weather w = new Weather();
            try {
                Date date =dateFormat.parse(arg);
                c.setTime(date);
                w.setYear(c.get(Calendar.YEAR));
                w.setMonth(c.get(Calendar.MONTH) + 1);
                w.setDay(c.get(Calendar.DATE));
                int t = Integer.parseInt(temp.substring(0, temp.toString().lastIndexOf("c")));
                w.setTemperature(t);
                context.write(w, new IntWritable(t));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }
}