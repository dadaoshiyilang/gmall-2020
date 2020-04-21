package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtils
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    val dateFmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    // 创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_STARTUP))

    val startDStream: DStream[StartUpLog] = kafkaDStream.map(rd => {
      val startUplog: StartUpLog = JSON.parseObject(rd.value(), classOf[StartUpLog])
      // 获取log中的时间
      val ts: Long = startUplog.ts

      val dataStr = dateFmt.format(new Date(ts))

      println(">>>>>>>>><<<<<<<<<<>>>>"+dataStr)

      val dateSplits: Array[String] = dataStr.split(" ")

      startUplog.logDate = dateSplits(0)
      startUplog.logHour = dateSplits(1)
      startUplog
    })

    // 根据redis进行跨批次的去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(ssc,startDStream)

    val filterByMidLogDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

    filterByMidLogDStream.cache()

    DauHandler.saveMidToRedis(filterByMidLogDStream)

    //把数据写入hbase+phoenix
    filterByMidLogDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    // 启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
