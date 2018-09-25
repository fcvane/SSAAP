package com.iwc.DifferenDBOperations

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Auther fcvane
  * Date 2018/9/25
  */
class RunMain {

}

object RunMain extends App {

  val conf = new SparkConf().setMaster("local[4]").setAppName("RunMain")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val sqlContext = new SQLContext(sc)
  val db = DBManager
  // 换成入参
  //dbtype数据库类型 可多选; sqlFilePath 需要执行的sql文件的绝对路径
  val dbtype = "oracle,mysql"
  val sqlFilePath = "C:\\Git\\SparkSQLApplyAnyPlatforms\\src\\main\\scala\\com\\iwc\\DifferenDBOperations\\text.sql"
  // 数据源类型判断和表配置处理
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val array = dbtype.toLowerCase.split(",")
  for (i <- 0 until array.length) {
    if (array(i) == "kudu") {
      val kuduTable = properties.getProperty("kudu.tabname").split(",")
      for (j <- 0 until kuduTable.length)
        db.readKudu(sqlContext, kuduTable(j))
    }
    else if (array(i) == "hive") {
      val hiveTable = properties.getProperty("hive.tabname").split(",")
      for (j <- 0 until hiveTable.length)
        db.readHive(hiveContext, hiveTable(j))
    }
    else if (array(i) == "oracle") {
      val oraTable = properties.getProperty("oracle.tabname").split(",")
      val jdbcUrl = "jdbc:oracle:thin:@//10.45.28.250:1521/orcl"
      val username = "ogg"
      val password = "ogg"
      for (j <- 0 until oraTable.length)
        db.readOracle(sqlContext, jdbcUrl, username, password, oraTable(j))
    }
    else if (array(i) == "mysql") {
      val mysqlTable = properties.getProperty("mysql.tabname").split(",")
      val jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test"
      val username = "root"
      val password = "abc123"
      for (j <- 0 until mysqlTable.length)
        db.readMySQL(sqlContext, jdbcUrl, username, password, mysqlTable(j))
    }
  }
  // SQL文件读取
  val sqlFile = Source.fromFile(sqlFilePath, "UTF-8")
  val str = sqlFile.mkString.toString
  sqlFile.close()
  val sqlArray = str.split(";")

  for (i <- 0 until sqlArray.length ) {
    val sql = sqlArray(i)
    if (!sql.startsWith("#")) {
      println(s"[ RunMain ] start execute ...")
      if (dbtype.toLowerCase.contains("hive")) {
        hiveContext.sql(sql).show(10)
      }
      else {
        sqlContext.sql(sql).show(10)
      }
      println(s"[ RunMain ] execute finish !")
    }
  }
  sc.stop()
}