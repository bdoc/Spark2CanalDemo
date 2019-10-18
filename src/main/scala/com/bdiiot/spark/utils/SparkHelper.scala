package com.bdiiot.spark.utils

import java.util.Objects

import com.bdiiot.spark.constant.Global
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

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

  def getStreamsListener(): StreamingQueryListener = {
    new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Query started: " + event.id)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val source = event.progress.sources.headOption
        source.map(src => {
          HdfsHelper.apply(Global.PATH_SINK).dir2Hive

          val end_offset: String = src.endOffset
          val start_offset: String = src.startOffset
          if (Objects.equals(start_offset, end_offset)) {
            flag = false
          }
          println(s"Start Offset: ${start_offset}, End offset: ${end_offset}")
        })
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query terminated: " + event.id)
      }

    }
  }

}
