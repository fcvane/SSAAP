package com.iwc.DifferenDBOperations

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Auther fcvane
  * Date 2018/9/25
  */
class RunMain {

}

object RunMain extends App {

  val conf = new SparkConf().setMaster("local[4]").setAppName("CommSQL")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)

}