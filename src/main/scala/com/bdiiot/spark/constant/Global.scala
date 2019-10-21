package com.bdiiot.spark.constant

class Global {
}

object Global {
  final val COL_COMMON_KEY = "id"
  final val COL_FLAG_KEY = "flag"
  final val COL_COMMON_UPDATE_TIME = "modify_date"

  final val DB_NAME = "canal"
  final val DB_MARK = "_bd_"

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
  final val PATH_CHECKPOINT = HDFS + "/tmp/checkpoint/"
  final val PATH_SINK = "/tmp/mysql2ods/sink/"
  final val PATH_INVALID = "/tmp/mysql2ods/invalid"
}