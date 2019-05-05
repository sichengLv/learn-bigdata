package com.lv.bigdata.action.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * @author lvsicheng
 * @date 2019-05-05 16:22
 */
public class ClickEventJavaStreaming {

    private static final Logger LOGGER = Logger.getLogger(ClickEventJavaStreaming.class);

    public static void startStreaming(SparkSession spark) {
        // 连接 Kafka
        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "mas130:9092,s131:9092")
                .option("subscribe", "test-flume02")
                .load();

        // 统一将字段转换为 Stringe类型
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> lineDS = kafkaDF.selectExpr("CAST(value AS STRING)").as(stringEncoder);

        // 将数据转换为 ClickEventDS
        Encoder<ClickEventLog> clickEventLogEncoder = Encoders.bean(ClickEventLog.class);
        Dataset<ClickEventLog> clickEventDS = lineDS.map((MapFunction<String, ClickEventLog>) line -> {
            String[] pair = line.split("\\s+");
            if (pair.length < 6) {
                throw new IllegalArgumentException("This line is invalid, line: " + line);
            }

            ClickEventLog clickEventLog = new ClickEventLog(pair[0].trim(), pair[1].trim(), pair[2].trim(), pair[3].trim(), pair[4].trim(), pair[5].trim());
            return clickEventLog;
        }, clickEventLogEncoder);

        // 进行聚合操作
        Dataset<Row> resultDF = clickEventDS.groupBy("queryWord").count()
                .toDF("topicName", "topicCount");

        // 创建输出流
        String url = "jdbc:mysql://localhost:3306/bigdata?useSSL=false";
        String username = "root";
        String password = "root";
        JdbcWriter jdbcWriter = new JdbcWriter(url, username, password);

        DataStreamWriter<Row> dataStreamWriter = resultDF.writeStream()
                .foreach(jdbcWriter)
                .outputMode("complete");

        // 执行
        try {
            dataStreamWriter.start().awaitTermination();
        } catch (StreamingQueryException e) {
            LOGGER.error("Data Stream Writer exception", e);
        }
    }


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("ClickEventJavaStreaming")
                .config("spark.sql.warehouse.dir", "G:/BaiduNetdiskDownload/learn/learn-bigdata/spark-warehouse")
                .getOrCreate();

        startStreaming(spark);

    }

}
