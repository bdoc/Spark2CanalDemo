package com.bdiiot.spark.streaming

import java.sql.Statement

import com.bdiiot.spark.utils.{HbaseHelper, PhoenixHelper}
import org.apache.spark.sql.ForeachWriter

object ForeachWriterPhoenix {

  def apply(): ForeachWriter[String] = {
    new ForeachWriterPhoenix()
  }
}

class ForeachWriterPhoenix() extends ForeachWriter[String] {
  private var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    statement = PhoenixHelper.getPhoenixConnection().createStatement()
    statement.execute(s"upsert into test.test_hive values (1, '${partitionId}', '2019-11-11 00:00:00', '2019-11-13 00:00:00'")
    true
  }

  override def process(value: String): Unit = {
    val sql: String = s"upsert into test.test_hive values (1, 'a', '2019-11-11 00:00:00', '2019-11-13 00:00:00'"
    statement.execute(sql)
  }

  override def close(errorOrNull: Throwable): Unit = {
    HbaseHelper.close
    statement.close()
  }
}
