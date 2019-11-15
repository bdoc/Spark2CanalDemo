package com.bdiiot.spark.constant

class Global {
}

object Global {
  final val PHOENIX_JDBC="jdbc:phoenix:bigdata.t01.58btc.com,bigdata.t02.58btc.com,bigdata.t03.58btc.com:2181:/hbase-secure"
  final val HBASE_TEST_TABLE="TEST:TEST_HBASE"
  final val DEFAULT_CF="0"

  final val BD_KEY = "database_name"
  final val TABLE_KEY = "table_name"
  final val CREATE_TIME_KEY = "created_date"
  final val UPDATE_TIME_KEY = "modified_date"
  final val PRIMARY_KEY = "primary_key"
  final val FLAG_KEY = "flag"

  final val DB_NAME = "ods"
  final val DB_MARK = "_bd_"
  final val DB_TABLE = "test.table_columns"

  final val DATABASE = "database"
  final val TABLE = "table"
  final val TYPE = "type"
  final val DATA = "data"
  final val KEY = "key"

  final val TRUE = true
  final val FALSE = false
  final val LEFT_OUTER = "left_outer"
  final val LEFT_ANTI = "left_anti"
  final val REPARTITION = 3
  final val HIVE_SOURCE = "hive"
  final val KAFKA_SOURCE = "kafka"
  final val TEXT_SOURCE = "text"
  final val CONSOLE_SOURCE = "console"

  final val HDFS = "hdfs://bigdata.t01.58btc.com"
  final val PATH_CHECKPOINT = HDFS + "/tmp/checkpoint_realtime/"
  final val PATH_SINK = "/tmp/mysql_to_ods/sink/"
  final val PATH_INVALID = "/tmp/mysql_to_ods/invalid"
}