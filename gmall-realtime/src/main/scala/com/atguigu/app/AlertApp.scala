package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 读取事件日志主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_EVENT))

    val sdfFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")

    // 将每行数据转换为样例类对象
    val eventDStream: DStream[EventLog] = kafkaDStream.map(rd => {
      val eventLog = JSON.parseObject(rd.value(), classOf[EventLog])
      val dateStr = sdfFormat.format(new Date(eventLog.ts))
      val dateArry = dateStr.split(" ")

      eventLog.logDate = dateArry(0)
      eventLog.logHour = dateArry(1)
      eventLog
    })
    // 开窗口
    val eventLogWindowDStream: DStream[EventLog] = eventDStream.window(Minutes(5))

    val groupEventLogDStream: DStream[(String, Iterable[EventLog])] = eventLogWindowDStream.map(log =>(log.mid,log)).groupByKey()

    val boolToAlertInfo: DStream[(Boolean, CouponAlertInfo)] = groupEventLogDStream.map { case (mid, logIter) =>

      // 创建集合存放领券的UID
      val uids = new util.HashSet[String]()
      val items = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()

      // 记录是否有点击行为
      var noClick: Boolean = true

      breakable {
        logIter.foreach(eventLog => {
          eventList.add(eventLog.evid)
          // 判断是否为点击行为
          if ("clickItem".equals(eventLog.evid)) {
            noClick = false
            // 跳出循环
            break()
          } else if ("coupon".equals(eventLog.evid)) {
            uids.add(eventLog.uid)
            items.add(eventLog.itemid)
          }
        })
      }

      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, items, eventList, System.currentTimeMillis()))
    }
    // 需要预警的日志
    val alertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = boolToAlertInfo.filter(_._1)

    val docIdToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfoDStream.map { case (_, alerInfo) => {

      val mid: String = alerInfo.mid

      val ts: Long = alerInfo.ts

      val dateStr: String = sdf.format(new Date(ts))

      (s"${mid}_$dateStr", alerInfo)

    }
    }

    // 测试打印
    //docIdToAlertInfoDStream.print()

    docIdToAlertInfoDStream.foreachRDD( rdd => {

      rdd.foreachPartition( iter => {

        MyEsUtil.insertBulk(GmallConstant.GMALL_COUPON_ALERT + sdf1.format(new Date(System.currentTimeMillis())),iter.toList)

      })

    })



    ssc.start()
    ssc.awaitTermination()
  }
}
