package com.iwc.DifferenDBOperations

import java.util.Properties

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Auther fcvane
  * Date 2018/9/25
  */
class DBManager {

}

object DBManager extends App {
  /**
    * 数据源管理
    */

  /**
    * Kudu
    * @param sqlContext
    * @param tabName
    */
  def readKudu(sqlContext: SQLContext, tabName: String): Unit = {
    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream("/config.properties"))
    val df = sqlContext.read.options(
      Map(
        "kudu.master" -> properties.getProperty("kudu.master"),
        "kudu.table" -> tabName
      )).kudu
    // 注册临时表
    df.createOrReplaceTempView(tabName)
  }

  /**
    * Hivw
    * @param hiveContext
    * @param tabName
    */
  def readHive(hiveContext: HiveContext, tabName: String): Unit = {
    hiveContext.table(tabName).createOrReplaceTempView(tabName)
  }

  /**
    * Oracle
    * @param sqlContext
    * @param jdbcUrl
    * @param userName
    * @param passWord
    * @param tabName
    */
  def readOracle(sqlContext: SQLContext, jdbcUrl: String, userName: String, passWord: String, tabName: String): Unit = {
    val oracleDF = Map(
      "url" -> jdbcUrl,
      "user" -> userName,
      "password" -> passWord,
      "dbtable" -> tabName,
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val df = sqlContext.read.options(oracleDF).format("jdbc").load
    df.createOrReplaceTempView(tabName)
  }

  /**
    * MySQL
    * @param sqlContext
    * @param jdbcUrl
    * @param userName
    * @param passWord
    * @param tabName
    */
  def readMySQL(sqlContext: SQLContext, jdbcUrl: String, userName: String, passWord: String, tabName: String): Unit = {
    val mysqlDF = Map(
      "url" -> jdbcUrl,
      "dbtable" -> tabName,
      "user" -> userName,
      "password" -> passWord,
      "driver" -> "com.mysql.jdbc.Driver")
    val df = sqlContext.read.options(mysqlDF).format("jdbc").load
    df.createOrReplaceTempView(tabName)
  }
}