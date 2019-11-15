package com.bdiiot.spark.streaming

import com.bdiiot.spark.constant.Global._
import com.bdiiot.spark.utils.HbaseHelper
import org.apache.commons.compress.utils.CharsetNames
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.spark.sql.ForeachWriter

object ForeachWriterHbase {

  def apply(): ForeachWriter[String] = {
    new ForeachWriterHbase()
  }
}

class ForeachWriterHbase() extends ForeachWriter[String] {
  private var hbaseConnection: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    hbaseConnection = HbaseHelper.getHbaseConnection()
    true
  }

  override def process(value: String): Unit = {
    val table = hbaseConnection.getTable(TableName.valueOf(HBASE_TEST_TABLE))
    val put = new Put("1".getBytes(CharsetNames.UTF_8))
    put.addColumn(DEFAULT_CF.getBytes(), "NAME".getBytes(), value.getBytes())
    put.addColumn(DEFAULT_CF.getBytes(), "CREATE_DATE".getBytes(), "2019".getBytes())
    put.addColumn(DEFAULT_CF.getBytes(), "MODIFY_DATE".getBytes(), "2019".getBytes())
    table.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    hbaseConnection.close()
  }
}
