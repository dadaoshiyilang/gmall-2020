package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtils
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    var conf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    // 读取订单主题数据创建流
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_ORDER_INFO))

    // 将JSON数据转换为样例类对象，同时补充字段(创建日期，创建小时)
    val kafkaInfoDStream: DStream[OrderInfo] = kafkaDStream.map(rd => {
      // 将JSON数据转换为样例类对象
      val orderInfo = JSON.parseObject(rd.value(), classOf[OrderInfo])

      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)

      orderInfo.consignee_tel = tuple._1 + "*******"

      val dataArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dataArr(0)
      orderInfo.create_hour = dataArr(1).split(":")(0)
      orderInfo
    })

    // 写入HBase(Phoenix)
    kafkaInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    // 开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
