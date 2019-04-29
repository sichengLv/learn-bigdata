package com.lv.learn.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class Base {

  private val conf = new SparkConf()
  conf.setMaster("local[4]")
  conf.setAppName("test")
  conf.set("spark.sql.warehouse.dir", "G:/BaiduNetdiskDownload/learn/learn-bigdata/spark-warehouse")

  protected final val sc = new SparkContext(conf)

  protected final val spark = SparkSession.builder().getOrCreate()

}
