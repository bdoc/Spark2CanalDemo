package com.bdiiot.spark.streaming

import com.bdiiot.spark.constant.Global
import com.bdiiot.spark.utils.{JsonHelper, SparkHelper}
import org.apache.spark.sql
import org.apache.spark.sql.streaming.Trigger

object StructuredKafka {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |
        """.stripMargin)
      System.exit(1)
    }

    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/tmp/kafka_client_jaas.conf")

    val Array(brokers, topics) = args

    val spark = SparkHelper.getSparkSession()
    import spark.implicits._

    val kafkaSource: sql.DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("startingOffsets", "earliest")
      .load()

    val allTableInfo = kafkaSource
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .flatMap(jsonStr => {
        JsonHelper.readJson(jsonStr)
      })
      .toDF("key", "data")

    import scala.concurrent.duration._
    // output to console
//    val job1 = allTableInfo
//      .writeStream
//      .trigger(Trigger.ProcessingTime(10.seconds))
//      .format("console")
//      .option("checkpointLocation", Global.PATH_CHECKPOINT + "mysql2ods")
//      .start()

    // output to hdfs
    val job2 = allTableInfo
      .writeStream
      .trigger(Trigger.ProcessingTime(30.seconds))
      .format("text")
      .option("path", Global.PATH_SINK)
      .option("checkpointLocation", Global.PATH_CHECKPOINT + "mysql2ods")
      .partitionBy("key")
      .start()

    SparkHelper.close

  }

}
