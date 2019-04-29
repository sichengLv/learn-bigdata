package com.lv.learn.scala

import org.apache.spark.sql.SparkSession

object df extends Base with Serializable {

  case class Tran(accNo: String, tranAmount: Double)

  def reflectCreate(spark: SparkSession): Unit = {

    //导入隐饰操作, 否则RDD无法调用toDF方法
    import spark.implicits._

    val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30", "SB10010,7000", "CR10001,7000", "SB10002,-10")

    // 通过 toDF()将 rdd 转换成 df
    val df = sc.parallelize(acTransList).map(_.split(",")).map(data => Tran(data(0).trim(), data(1).trim().toDouble)).toDF()
    df.createOrReplaceTempView("tran")
    df.printSchema()
    df.show()

    // 通过 createDataFrame(rdd) 来创建 df
    val rdd = sc.parallelize(acTransList).map(_.split(",")).map(data => Tran(data(0).trim(), data(1).trim().toDouble))
    val df2 = spark.createDataFrame(rdd)
    df2.createOrReplaceTempView("tran2")
    df2.show()

    // 通过 Seq 来创建 df
    def toTrans(trans: Seq[String]): Tran = Tran(trans(0), trans(1).trim.toDouble)

    val test = sc.parallelize(acTransList).map(_.split(",")).map(toTrans(_))
    val df3 = spark.createDataFrame(test)
    df3.createOrReplaceTempView("tran3")
    spark.sql("select * from tran3").show

  }

  def main(args: Array[String]): Unit = {
    reflectCreate(spark)
  }


}

