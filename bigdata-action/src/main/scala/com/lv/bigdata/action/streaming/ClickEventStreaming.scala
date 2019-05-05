package com.lv.bigdata.action.streaming

import org.apache.spark.sql.SparkSession

/**
  * 消费 Kafka消息并对数据进行处理和计算, 然后将结果插入到 mysql中
  *
  * 运行 Spark本地模式就可以了
  */
object ClickEventStreaming {

  /**
    * 定义 ClickEvent
    */
  case class ClickEvent(visitTime: String,
                        uid: String,
                        queryWord: String,
                        rank: String,
                        seq: String,
                        url: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("click event streaming")
      .config("spark.sql.warehouse.dir", "G:/BaiduNetdiskDownload/learn/learn-bigdata/spark-warehouse")
      .getOrCreate()

    // 构造 Kafka DF
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "mas130:9092,s131:9092")
      .option("subscribe", "test-flume02")
      .load()

    // 将数据转换为 ClickEvent DF
    import spark.implicits._
    val lineDS = df.selectExpr("CAST(value AS STRING)").as[String]
    val clickEventDS = lineDS.map(_.split("\\s+")).map(x => ClickEvent(x(0), x(1), x(2), x(3), x(4), x(5)))
    val topicCountDF = clickEventDS.groupBy("queryWord").count().toDF("topicName", "topicCount")

    val url = "jdbc:mysql://localhost:3306/bigdata?useSSL=false"
    val username = "root"
    val password = "root"

    // 创建 ForeachWriter
    val writer = new JdbcSink(url, username, password)

    // 写数据
    val topicCount = topicCountDF
      .writeStream
      .foreach(writer)
      .outputMode("complete")

    // 阻塞等待程序执行结束
    topicCount.start().awaitTermination()
  }

}
