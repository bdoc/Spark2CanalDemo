package com.bdiiot.spark.utils

class Constant {
}

object Constant {
  final val PHOENIX_JDBC = "jdbc:phoenix:bigdata.t01.58btc.com,bigdata.t02.58btc.com,bigdata.t03.58btc.com:2181:/hbase-secure:hbase-bd@58BTC.COM:/etc/security/keytabs/hbase.headless.keytab"
  final val PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"

  final val HBASE_TEST_TABLE = "TEST:TEST_HBASE"
  final val DEFAULT_CF = "0"

  final val OUTPUT_MODE = "update"

  final val DATABASE = "database"
  final val TABLE = "table"
  final val TYPE = "type"
  final val DATA = "data"
  final val KEY = "key"

  final val REPARTITION = 3
  final val HIVE_SOURCE = "hive"
  final val KAFKA_SOURCE = "kafka"
  final val TEXT_SOURCE = "text"
  final val CONSOLE_SOURCE = "console"

  final val HDFS = "hdfs://bigdata.t01.58btc.com"
  final val PATH_CHECKPOINT = HDFS + "/tmp/checkpoint_realtime/"
}