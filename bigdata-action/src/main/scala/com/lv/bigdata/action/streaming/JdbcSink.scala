package com.lv.bigdata.action.streaming

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * jdbc sink
  *
  * 接收 spark streaming 处理过的数据, 输出到 mysql中
  */
class JdbcSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  // _的含义: 初始化对象
  var statement: Statement = _
  var resultSet: ResultSet = _
  var connection: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement()
    return true
  }

  override def process(value: Row): Unit = {
    println(value)

    val topic = value.getAs[String]("titleName").replaceAll("[\\[\\]]", "")
    val webCount = value.getAs[Long]("webCount")

    val querySql = "select * from click_event where topic = '" + topic + "'"
    val updateSql = "update click_evnet set count = " + webCount + " where topic = '" + topic + "'"
    val insertSql = "insert into click_event(topic, count)" + " values('" + topic + "," + webCount + ")"

    val resultSet = statement.executeQuery(querySql)
    try {
      if (resultSet.next()) {
        statement.executeUpdate(updateSql)
      } else {
        statement.execute(insertSql)
      }
    } catch {
      case ex: Exception => {
        println("Excepion")
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (statement != null) {
      statement.close()
    }

    if (connection != null) {
      connection.close()
    }
  }
}
