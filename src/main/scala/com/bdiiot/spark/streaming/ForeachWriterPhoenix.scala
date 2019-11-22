package com.bdiiot.spark.streaming

import java.sql.{Connection, Statement}

import com.bdiiot.spark.utils.PhoenixHelper
import org.apache.spark.sql.ForeachWriter

object ForeachWriterPhoenix {

  def apply(): ForeachWriter[String] = {
    new ForeachWriterPhoenix()
  }
}

class ForeachWriterPhoenix() extends ForeachWriter[String] {
  private var connection: Connection = _
  private var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    connection = PhoenixHelper.getPhoenixConnection
    statement = connection.createStatement()
    true
  }

  override def process(value: String): Unit = {
    val sql: String = s"upsert into test.test_hive values (1, 'a', '2019-11-11 00:00:00', '2019-11-13 00:00:00'"
    statement.execute(sql)
    connection.commit()
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      statement.close()
    } catch {
      case ex: Exception =>
        println(s"close statementfailed, msg=$ex")
    }
    try {
      connection.close()
    } catch {
      case ex: Exception =>
        println(s"close statement failed, msg=$ex")
    }
    PhoenixHelper.close()
  }
}
