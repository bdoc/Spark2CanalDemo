package com.bdiiot.spark.utils

import com.bdiiot.spark.constant.Global
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {

  private var singleSparkSession: SparkSession = null

  def getSparkSession(): SparkSession = {
    synchronized {
      if (singleSparkSession == null) {
        val conf = new SparkConf()
          .setAppName("mysql2ods")

        singleSparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
        singleSparkSession.sparkContext.setCheckpointDir(Global.PATH_CHECKPOINT + "spark")
      }
    }
    singleSparkSession
  }

  private var flag: Boolean = true

  def close: Unit = {
    while (flag) {
      Thread.`yield`()
    }

    if (singleSparkSession != null) {
      try {
        singleSparkSession.close()
      } catch {
        case ex: Exception => {
          println(s"close singled sparksession failed, msg=$ex")
        }
      }
    }

  }

}
