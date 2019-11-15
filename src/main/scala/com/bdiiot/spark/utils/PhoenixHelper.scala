package com.bdiiot.spark.utils

import java.sql.{Connection, DriverManager}

import com.bdiiot.spark.constant.Global._


object PhoenixHelper {

  private var singlePhoenixConnection: Connection = _

  def getPhoenixConnection(): Connection = {
    synchronized {
      if (singlePhoenixConnection == null) {
        println("create phoenix connection start ...")
        singlePhoenixConnection = DriverManager.getConnection(PHOENIX_JDBC)
        println("create phoenix connection success!")
      }
    }
    singlePhoenixConnection
  }

  def close: Unit = {
    if (singlePhoenixConnection != null) {
      try {
        singlePhoenixConnection.close()
      } catch {
        case ex: Exception => {
          println(s"close singled hbaseConnection failed, msg=$ex")
        }
      }
    }

  }

}

