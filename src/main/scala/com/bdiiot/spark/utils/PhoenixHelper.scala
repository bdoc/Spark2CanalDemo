package com.bdiiot.spark.utils

import java.sql.{Connection, DriverManager}

import com.bdiiot.spark.utils.Constant._


object PhoenixHelper {

  private var singlePhoenixConnection: Connection = _

  def getPhoenixConnection: Connection = {
    synchronized {
      if (singlePhoenixConnection == null) {
        Class.forName(PHOENIX_DRIVER)
        singlePhoenixConnection = DriverManager.getConnection(PHOENIX_JDBC)
      }
    }
    singlePhoenixConnection
  }

  def close(): Unit = {
    if (singlePhoenixConnection != null) {
      try {
        singlePhoenixConnection.close()
      } catch {
        case ex: Exception =>
          println(s"close singled hbaseConnection failed, msg=$ex")
      }
    }

  }

}

