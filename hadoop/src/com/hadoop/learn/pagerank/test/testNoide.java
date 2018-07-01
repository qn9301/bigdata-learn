package com.hadoop.learn.pagerank.test;

import com.hadoop.learn.pagerank.Node;
import org.junit.Test;

public class testNoide {
    /**
     *   3
         true
         A
         A:1.0  B   C
         [B, C]
         1.0
         true
         A:0.6	B	C
     */
    @Test
    public void test(){
        Node node = new Node("A:1.0   B   C");
        System.out.println(node.unserialize());
        System.out.println(node.getKey());
        System.out.println(node.getStrData());
        System.out.println(node.getVoteNode().toString());
        System.out.println(node.getWeight());
        System.out.println(node.isChain());
        node.setWeight(0.6);
        System.out.println(node.toString());
    }

}
