package com.iwc.DifferenDBOperations

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Auther fcvane
  * Date 2018/9/25
  */
class SQLManager {

}

object SQLManager extends App {
  /**
    * 文件流管理
    */
  //  def SQLManager(sql:String): Set[String] ={
  //    val
  //    val array = sql.split(";")
  //    for (i<-0 to array.len{
  //
  //    }
  //  }
  val conf = new SparkConf().setAppName("test").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val a = Source.fromFile("C:\\Git\\SparkSQLApplyAnyPlatforms\\src\\main\\scala\\com\\iwc\\DifferenDBOperations\\text.sql", "utf-8")
  val b = a.mkString.toString.split(";")
  for (i <- 0 until b.length) {
    println(b(i), "-----------------------")
    val c = b(i)
    hiveContext.sql(c).show()

  }
}