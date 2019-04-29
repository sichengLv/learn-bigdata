package com.lv.learn.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class BaseSpark {

    // 创建spark配置对象
    private static SparkConf conf = new SparkConf();

    // 创建spark上下文对象
    protected JavaSparkContext sc;


    public BaseSpark() {
        conf.setAppName("base");
        // 本地模式, 开启4个worker线程
        conf.setMaster("local[4]");

        sc = new JavaSparkContext(conf);
    }

}
