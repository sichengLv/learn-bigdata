package com.lv.learn.spark.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author lvsicheng
 * @date 2019-05-01 17:37
 */
public class JavaSparkSQLExample {

    /**
     * 创建一个Person类
     */
    public static class Person implements Serializable {

        private String name;

        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[3]")
                .appName("Spark SQL Example")
                .config("spark.sql.warehouse.dir", "G:/BaiduNetdiskDownload/learn/learn-bigdata/spark-warehouse")
                .getOrCreate();

        // 创建 DataFrame, 可以通过 RDD, Hive表 和通过 spark数据源来创建
        Dataset<Row> df = spark.read().json(JavaSparkSQLExample.class.getResource("/") + "people.json");
//        df.show();
//
//        // Untyped Dataset Operations
//        df.printSchema();
//        df.select("name").show();
//        df.select(df.col("name"), df.col("age").plus(1)).show();
//        df.filter(df.col("age").gt(21)).show();
//        df.groupBy("age").count().show();

        // SQL Query

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();

        // Creating Datasets
        Person person = new Person();
        person.setName("luffy");
        person.setAge(23);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> intDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = intDS.map((MapFunction<Integer, Integer>) a -> a + 1, integerEncoder);
        transformedDS.show();

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        Dataset<Person> personDS = spark.read().json(JavaSparkSQLExample.class.getResource("/") + "people.json").as(personEncoder);
        personDS.show();

    }


}
