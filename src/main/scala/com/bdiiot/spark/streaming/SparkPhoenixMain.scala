package com.bdiiot.spark.streaming

import com.bdiiot.spark.utils.Constant._
import com.bdiiot.spark.utils.SparkHelper
import org.apache.spark.sql

object SparkPhoenixMain {
  {

  }

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

    val spark = SparkHelper.getSparkSession

    val kafkaSource: sql.DataFrame = spark
      .readStream
      .format(KAFKA_SOURCE)
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("kafka.security.protocol", security)
      .option("startingOffsets", offsets)
      .load()

    import spark.implicits._
    val kafkaSourceString = kafkaSource.selectExpr("CAST(value AS STRING)").as[String]

    // output to console
    //    val query = kafkaSourceString
    //      .writeStream
    //      .format(CONSOLE_SOURCE)
    //      .option("checkpointLocation", PATH_CHECKPOINT + "mysql_to_ods")
    //      .start()

    val query = kafkaSourceString.writeStream
      .foreach(PhoenixForeachWriter.apply())
      .outputMode(OUTPUT_MODE)
      .option("checkpointLocation", PATH_CHECKPOINT + "mysql_to_ods")
      .start()

    query.awaitTermination()
    SparkHelper.close()
  }

}
