package com.lv.learn.scala

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


/**
  * 创建 DataFrame 的多种方式
  */
object CreateDataFrame extends Base with Serializable {

  /**
    * 定义 Student 样例类, 用于模式匹配
    */
  case class Stu(sid: Int, name: String, age: Int)

  /**
    * 方法一: 根据 case class 来创建
    */
  def rddToDFCase(): DataFrame = {
    //导入隐饰操作, 否则RDD无法调用toDF方法
    import spark.implicits._

    val stuRDD = spark.sparkContext
      .textFile("G:\\BaiduNetdiskDownload\\learn\\learn-bigdata\\learn-spark\\src\\main\\resources\\stu.txt")
      .map(_.split(",")).map(stu => Stu(stu(0).trim.toInt, stu(1).trim, stu(2).trim.toInt))

    // 转换成 DF
    val stuDF = stuRDD.toDF()
    return stuDF
  }

  /**
    * 方法二: 根据 Seq 来创建
    */
  def rddToDFSeq(): DataFrame = {

    // 设置 schema 结构
    val schemaString = "id,name,age"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true))
    val schema = StructType(fields)

    val stuRDD = spark.sparkContext
      .textFile("G:\\BaiduNetdiskDownload\\learn\\learn-bigdata\\learn-spark\\src\\main\\resources\\stu.txt")

    val rowRDD = stuRDD.map(_.split(",")).map(parts => Row(parts(0), parts(1), parts(2)))
    val stuDF = spark.createDataFrame(rowRDD, schema)
    return stuDF
  }

  def main(args: Array[String]): Unit = {
    val df1 = rddToDFCase()
    df1.printSchema()
    df1.show()

    val df2 = rddToDFSeq()
    df2.printSchema()
    df2.show()
  }

}
