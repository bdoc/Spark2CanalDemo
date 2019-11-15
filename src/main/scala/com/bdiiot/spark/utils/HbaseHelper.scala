package com.bdiiot.spark.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseHelper {

  private var singleHbaseConnection: Connection = _

  def getHbaseConnection(): Connection = {
    synchronized {
      if (singleHbaseConnection == null) {
        val conf = HBaseConfiguration.create()
        singleHbaseConnection = ConnectionFactory.createConnection(conf)
      }
    }
    singleHbaseConnection
  }

  def close: Unit = {
    if (singleHbaseConnection != null) {
      try {
        singleHbaseConnection.close()
      } catch {
        case ex: Exception => {
          println(s"close singled hbaseConnection failed, msg=$ex")
        }
      }
    }

  }

}

