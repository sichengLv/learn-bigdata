package com.lv.learn.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WordCountDemo extends BaseSpark implements Serializable {

    public static void main(String[] args) {

        Map<String, String> map = new HashMap<>();
        Map<String, String> map2 = new ConcurrentHashMap<>();

        // 创建spark配置对象
        SparkConf conf = new SparkConf();
        conf.setAppName("word count");
        // 本地模式, 开启4个worker线程
        conf.setMaster("local[4]");

        // 创建spark上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建rdd
        JavaRDD<String> rdd = sc.textFile("g:/work/word.txt");

        // flatMap, 将单词炸开, 变成一维数组
        JavaRDD<String> flatRdd = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                for (String str : s.split(" ")) {
                    list.add(str);
                }
                return list.iterator();
            }
        });

//        List<String> collect = flatRdd.collect();
//        for (String str : collect) {
//            System.out.println(str);
//        }

        // word => (word, 1)
        JavaPairRDD<String, Integer> wordRdd = flatRdd.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             * word => (word, 1)
             * @param s 输入
             * @return 元组
             * @throws Exception
             */
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 按照key进行分发
        // new Function2的三个参数分别表示, 第一个是call方法的返回值, 第二个和第三个是方法的输入参数(同一个key中的value相加)
        // reduceByKey的返回值类型是<String, Integer>, 是根据wordRdd自动生成的, 因为key类型为String, value为Integer
        JavaPairRDD<String, Integer> resultRdd = wordRdd.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        // 输出结果
        List<Tuple2<String, Integer>> res = resultRdd.collect();
        for (Tuple2<String, Integer> tuple : res) {
            System.out.println(tuple._1() + "," + tuple._2());
        }

    }

}
