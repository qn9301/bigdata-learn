package com.hadoop.learn.pagerank;

public class Const {
    // q
    public static double Q = 0.85;
    // 阈值
    public static double THRESHOLD = 0.001;
    // 可以创建一个MapReduce程序先把行数count出来
    public static int TOTAL = 4;
    // 定义文件前缀
    public static String JOB_PREFIX = "zyf";
    // 定义count key
    public static String COUNT_KEY = "COUNT_KEY";
    //
    public static String SUM_KEY = "SUM_KEY";
}
