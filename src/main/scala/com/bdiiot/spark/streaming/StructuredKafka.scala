package com.bdiiot.spark.streaming

import com.bdiiot.spark.constant.Global._
import com.bdiiot.spark.utils.{JsonHelper, SparkHelper}
import org.apache.spark.sql
import org.apache.spark.sql.streaming.Trigger

object StructuredKafka {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |  <offsets> auto.offset.reset [latest, earliest, none]
          |  <security> kafka.security [PLAINTEXT, SASL_PLAINTEXT]
          |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics, offsets, security) = args

    if (security == "SASL_PLAINTEXT") {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("java.security.auth.login.config", "/tmp/kafka_client_jaas.conf")
    }

    val spark = SparkHelper.getSparkSession()
    spark.streams.addListener(SparkHelper.getStreamsListener())

    import spark.implicits._

    val kafkaSource: sql.DataFrame = spark
      .readStream
      .format(KAFKA_SOURCE)
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("kafka.security.protocol", security)
      .option("startingOffsets", offsets)
      .load()

    val allTableInfo = kafkaSource
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .flatMap(jsonStr => {
        JsonHelper.readJson(jsonStr)
      })
      .toDF(KEY, DATA)

    import scala.concurrent.duration._
    // output to console
    //    val job1 = allTableInfo
    //      .writeStream
    //      .trigger(Trigger.ProcessingTime(10.seconds))
    //      .format(CONSOLE_SOURCE)
    //      .option("checkpointLocation", PATH_CHECKPOINT + "mysql2ods")
    //      .start()

    // output to hdfs
    val job2 = allTableInfo
      .writeStream
      .trigger(Trigger.ProcessingTime(3.minutes))
      .format(TEXT_SOURCE)
      .option("path", PATH_SINK)
      .option("checkpointLocation", PATH_CHECKPOINT + "mysql2ods")
      .partitionBy(KEY)
      .start()

    SparkHelper.close

  }

}
