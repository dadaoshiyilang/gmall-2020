package com.atguigu.utils

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtils {

  private val properties: Properties = PropertiesUtil.load("config.properties")

  // ctrl + alt + V
  private val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
      ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> properties.getProperty("kafka.value.deserializer")
  )

  def getKafkaStream(ssc:StreamingContext,topics:Set[String])={

    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    kafkaDstream
  }

}
