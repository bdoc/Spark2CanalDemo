package com.bdiiot.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {

  private var singleSparkSession: SparkSession = _

  def getSparkSession: SparkSession = {
    synchronized {
      if (singleSparkSession == null) {
        val conf = new SparkConf()
          .setAppName("mysql_to_ods_realtime")

        singleSparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
        singleSparkSession.sparkContext.setCheckpointDir(Constant.PATH_CHECKPOINT + "spark")
      }
    }
    singleSparkSession
  }

  def close(): Unit = {
    if (singleSparkSession != null) {
      try {
        singleSparkSession.close()
      } catch {
        case ex: Exception =>
          println(s"close singled sparksession failed, msg=$ex")
      }
    }
  }
}
