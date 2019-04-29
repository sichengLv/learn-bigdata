package com.lv.learn.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class CombinerDemo extends BaseSpark {

    public void testAvg() {

        /*// 1.创建createCombiner函数
        PairFunction<Integer, Integer, AvgCount> createCombiner = new PairFunction<Integer, Integer, AvgCount>() {
            @Override
            public Tuple2<Integer, AvgCount> call(Integer x) throws Exception {
                return new Tuple2<>(x, new AvgCount(x, 1));
            }
        };

        // 2.创建mergeValue函数
        Function2<AvgCount, Integer, AvgCount> mergeValue = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount apply(AvgCount v1, Integer v2) {
                int total = v1.getTotal_() + v2;
                int num = v1.getNum_() + 1;
                return new AvgCount(total, num);
            }
        };

        // 3.创建mergeCombiners函数
        Function2<AvgCount, AvgCount, AvgCount> mergeCombiners = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount apply(AvgCount v1, AvgCount v2) {
                return new AvgCount(v1.getTotal_() + v2.getTotal_(), v1.getNum_() + v2.getNum_());
            }
        };

        // 开始计算
        AvgCount initial = new AvgCount(0, 0);

        // 构造javaRDD
        JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, 2),
                new Tuple2<>(2, 1),
                new Tuple2<>(1, 1),
                new Tuple2<>(2, 5),
                new Tuple2<>(3, 4),
                new Tuple2<>(3, 1)), 3);

        // 转换为pairRDD
//        JavaPairRDD<Integer, AvgCount> ers = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners);
*/
    }

}


class AvgCount implements Serializable {

    /**
     * 累计
     */
    private int total_;

    /**
     * 个数
     */
    private int num_;

    public int getTotal_() {
        return total_;
    }

    public void setTotal_(int total_) {
        this.total_ = total_;
    }

    public int getNum_() {
        return num_;
    }

    public void setNum_(int num_) {
        this.num_ = num_;
    }

    public AvgCount(int total, int num) {
        this.total_ = total;
        this.num_ = num;
    }

    /**
     * 求平均
     */
    public float avg() {
        return total_ / (float) num_;
    }

}
