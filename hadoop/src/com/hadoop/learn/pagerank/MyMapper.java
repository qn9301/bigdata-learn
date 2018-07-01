package com.hadoop.learn.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * A    B   C   D
 * =>
 * A:1.0    B   C   D
 * B:0.3333
 * C:0.3333
 * D:0.3333
 */
public class MyMapper extends Mapper<Text, Text, Text, Text>{
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // 获取循环次数
        int runCount= context.getConfiguration().getInt(Const.COUNT_KEY, 1);
        // 判断是不是第一次进入
        Node node;
        if (runCount == 1) {
            node = new Node(key.toString() + ":1.0\t" + value.toString());
        } else {
            node = new Node(key.toString() + "\t" + value.toString());
        }
        if (node.unserialize()){
            // 得到node
            List<String> list = node.getVoteNode();
            double score = 0;
            if (list.size() > 0) {
                score = node.getWeight() / list.size();
            }
            for (String s : list) {
                context.write(new Text(s), new Text(s + ":" + score));
            }
            context.write(new Text(node.getKey()), new Text(node.toString()));
        }
    }
}
