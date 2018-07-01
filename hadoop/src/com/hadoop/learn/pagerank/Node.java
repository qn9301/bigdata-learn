package com.hadoop.learn.pagerank;

import com.google.common.collect.Lists;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Node implements WritableComparable {

    private String separator = "\t";

    private String separator2 = ":";

    private String strData;

    private String key;

    private List<String> voteNode;

    private Double weight;

    public Node(String data){
        strData = data;
    }

    public boolean unserialize() {
        String[] str = StringUtils.split(strData, separator);
        List<String> list= Arrays.asList(str);
        if (list.size() == 0) {
            return false;
        }
        String tmp = list.get(0);
        String[] strs = tmp.split(separator2);
        key = strs[0];
        weight = Double.parseDouble(strs[1]);
        if (list.size() == 1) {
            voteNode = null;
        } else {
            voteNode = list.subList(1, list.size());
        }

        return true;
    }

    public String getStrData() {
        return strData;
    }

    public void setStrData(String strData) {
        this.strData = strData;
    }

    public String getKey() {
        return key;
    }

    /**
     * 判断这个数据是链数据，还是单节点的权重数据
     * 链数据 A:1.0    B   C
     * 权重数据 A:0.5
     * @return
     */
    public boolean isChain() {
        return voteNode != null;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<String> getVoteNode() {
        return voteNode;
    }

    public void setVoteNode(List<String> voteNode) {
        this.voteNode = voteNode;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    @Override
    public int compareTo(Object o) {
        Node node = (Node) o;
        int res = key.compareTo(node.getKey());
        if (res == 0) {
            return isChain() ? (node.isChain() ? 0 : 1) : -1;
        }
        return res;
    }

    public String toString() {
        if (isChain()) {
            return key + separator2 + weight + separator
                    + StringUtils.join(voteNode.toArray(), separator);
        }
        return key + separator2 + weight;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
