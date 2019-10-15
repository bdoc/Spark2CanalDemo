package com.bdiiot.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <groupId> is a group id
          |  <topics> is a list of one or more kafka topics to consume from
          |
        """.stripMargin)
      System.exit(1)
    }

    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/tmp/kafka_client_jaas.conf")

    val Array(brokers, groupId, topics) = args

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      "security.protocol" -> SecurityProtocol.SASL_PLAINTEXT.name
    )

    val messages = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topics), kafkaParams)
    )

    val lines = messages.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
