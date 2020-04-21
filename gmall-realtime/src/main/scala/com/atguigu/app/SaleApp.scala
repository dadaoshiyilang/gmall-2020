package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtils, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val sdfFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val orderInfoKafka: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_ORDER_INFO))

    val detailKafka: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_ORDER_DETAIL))

    val userInfoKafka: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(ssc, Set(GmallConstant.GMALL_USER_INFO))

    // 将用户信息保存到redis
    userInfoKafka.foreachRDD(rdd => {

      // 对RDD分区进行操作
      rdd.foreachPartition( irer => {

        // 获取redis连接
        val jedisClient = RedisUtil.getJedisClient

        // 对每个分配的数据遍历操作
        irer.foreach( rd => {
          val userInfJson: String = rd.value()
          // 转成样例类对象
          val userInfo = JSON.parseObject(userInfJson, classOf[UserInfo])
          jedisClient.set(s"user:${userInfo.id}", userInfJson)
        })
        jedisClient.close()
      })
    })



    // 订单信息转换为样例类
    val orderKafkaDStream: DStream[OrderInfo] = orderInfoKafka.map(rd => {
      val orderInfo: OrderInfo = JSON.parseObject(rd.value(), classOf[OrderInfo])
      // 手机号脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple + "*******"
      // 获取日期及小时
      val dateStr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateStr(0)
      orderInfo.create_hour = dateStr(1).split(":")(0)
      orderInfo
    })

    // 订单详情信息转换为样例类
    val detailKafkaDStream: DStream[OrderDetail] = detailKafka.map(rd => {
      JSON.parseObject(rd.value(), classOf[OrderDetail])
    })

    // 订单信息转换为kv结构
    val orderDStream: DStream[(String, OrderInfo)] = orderKafkaDStream.map(orderInfo => {
      (orderInfo.id, orderInfo)
    })

    // 订单明细转换为kv结构
    val orderDetailDStream: DStream[(String, OrderDetail)] = detailKafkaDStream.map(orderDetail => {
      (orderDetail.order_id, orderDetail)
    })

    // 全外连接
    val fullOrderInfoDetail: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderDStream.fullOuterJoin(orderDetailDStream)

    // 处理每一行数据，使用分区操作代替每一行数据操作，减少连接
    val fullOrderInfoDetailDStream: DStream[SaleDetail] = fullOrderInfoDetail.mapPartitions(iter => {
      val saleDetails = new ListBuffer[SaleDetail]
      // 获取redis连接
      val jedisClient = RedisUtil.getJedisClient

      implicit val formats = org.json4s.DefaultFormats
      iter.foreach {
        case (orderId, (orderInfoOpt, orderDetailOpt)) => {
          // 定义redisKey
          val orderInfoKey = s"order:$orderId"
          var orderDetailKey = s"orderDetail:$orderId"

          // 判断orderInfoOpt是否为空
          if (orderInfoOpt.isDefined) {
            // 获取orderInfo信息
            val orderInfo: OrderInfo = orderInfoOpt.get

            if (orderDetailOpt.isDefined) {
              // 获取orderDetail信息
              val orderDetail: OrderDetail = orderDetailOpt.get
              saleDetails += new SaleDetail(orderInfo, orderDetail)
            }

            // 将orderInfo数据转换为JSON写入到redis
            val orderInfoJson: String = Serialization.write(orderInfo)
            jedisClient.set(orderInfoKey, orderInfoJson)
            jedisClient.expire(orderInfoKey, 60)

            // 查询detail的缓存
            val orderDetails: util.Set[String] = jedisClient.smembers(orderDetailKey)

            import scala.collection.JavaConversions._
            orderDetails.foreach(orderDetail => {
              val detail: OrderDetail = JSON.parseObject(orderDetail, classOf[OrderDetail])
              saleDetails += new SaleDetail(orderInfo, detail)
            })
          } else {
            // orderInfo为空，获取orderDetail信息
            val orderDetail: OrderDetail = orderDetailOpt.get
            // 判断缓存中是否存在orderInfo信息
            if (jedisClient.exists(orderInfoKey)) {
              // 获取orderInfoKey对应的缓存信息
              val orderInfoJson: String = jedisClient.get(orderInfoKey)
              // 转换为样例类对象
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              saleDetails += new SaleDetail(orderInfo, orderDetail)
            } else {
              // 缓存中不存在该信息，写入缓存
              val orderDetailInfo: String = Serialization.write(orderDetail)
              jedisClient.sadd(orderDetailKey, orderDetailInfo)
              jedisClient.expire(orderDetailKey, 60)
            }
          }
        }
      }
      jedisClient.close()
      saleDetails.toIterator
    })



//    // 订单信息
//    val orderInfoDStream: DStream[OrderInfo] = orderInfoKafka.map(rd => {
//      val orderInfo: OrderInfo = JSON.parseObject(rd.value(), classOf[OrderInfo])
//      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
//      orderInfo.consignee_tel = telTuple._1 + "*******"
//      // 获取日期及小时字段
//      val dateStrs: Array[String] = orderInfo.create_time.split(" ")
//      orderInfo.create_date = dateStrs(0)
//      orderInfo.create_hour = dateStrs(1).split(":")(0)
//      orderInfo
//    })

//    // 订单详情
//    val detailDStream: DStream[OrderDetail] = detailKafka.map(rd => {
//      JSON.parseObject(rd.value(), classOf[OrderDetail])
//    })
//
//    // 将订单及订单详情转换为kv结构类型
//    val idToInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map(orderInfo => {
//      (orderInfo.id, orderInfo)
//    })
//    val idToDetailDStream: DStream[(String, OrderDetail)] = detailDStream.map(orderDetail => {
//      (orderDetail.order_id, orderDetail)
//    })

    // 全外连接
    //val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream.fullOuterJoin(idToDetailDStream)

    // 处理每一行数据，使用分区操作代替每一行数据操作，减少连接
    /* val userSaleDetail: DStream[SaleDetail] = joinDStream.mapPartitions(iter => {
      // 创建集合用于存放匹配上的结果数据
      val details = new ListBuffer[SaleDetail]
      // 获取redis连接
      val jedisClient = RedisUtil.getJedisClient

      implicit val formats = org.json4s.DefaultFormats

      iter.foreach {
        case (orderId, (orderInfoOpt, orderDetailOpt)) => {
          // 定义redisKey
          val orderInfoKey = s"order:$orderId"
          val orderDetailKey = s"detail:$orderId"

          // orderInfoOpt不为空
          if (orderInfoOpt.isDefined) {
            // 取出orderInfo
            val orderInfo: OrderInfo = orderInfoOpt.get
            // orderDetailOpt也不为空
            if (orderDetailOpt.isDefined) {
              // 取出orderDetail数据
              val orderDetail: OrderDetail = orderDetailOpt.get
              // 将orderInfo和orderDetail结合放入集合
              details += new SaleDetail(orderInfo, orderDetail)
            }

            // 将orderInfo数据转换为JSON写入到redis
            val orderInfoJson: String = Serialization.write(orderInfo)
            jedisClient.set(orderInfoKey, orderInfoJson)
            jedisClient.expire(orderInfoKey, 60)

            // 查询detail的缓存
            val detailset: util.Set[String] = jedisClient.smembers(orderDetailKey)
            import scala.collection.JavaConversions._
            detailset.foreach(detailJson => {
              val orderDetail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              details += new SaleDetail(orderInfo, orderDetail)
            })
          } else {
            // orderInfoOpt为空，直接获取orderDetailOpt数据
            val orderDetail: OrderDetail = orderDetailOpt.get
            // 判断缓存中是否有数据
            if (jedisClient.exists(orderInfoKey)) {
              val orderInfoJson: String = jedisClient.get(orderInfoKey)
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              details += new SaleDetail(orderInfo, orderDetail)
            } else {
              val orderDetailJson: String = Serialization.write(orderDetail)
              jedisClient.sadd(orderDetailKey, orderDetailJson)
              jedisClient.expire(orderDetailKey, 60)
            }
          }
        }
      }
      jedisClient.close()
      details.toIterator
    })*/

    // 根据user_id查询redis中的用户信息，给SaleDetail补全用户信息
    val saleDetailUserInfoDStream: DStream[SaleDetail] = fullOrderInfoDetailDStream.mapPartitions(iter => {

      val saleDetails = new ListBuffer[SaleDetail]

      val jedisClient = RedisUtil.getJedisClient

      // 对分区数据进行遍历
      iter.foreach(saleDetail => {

        val userInfoKey = s"user:${saleDetail.user_id}"

        if (jedisClient.exists(userInfoKey)) {

          val userInfoJson: String = jedisClient.get(userInfoKey)

          // 转换为样例类
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])

          // 将结合了用户信息的SaleDetail保存到集合中
          saleDetail.mergeUserInfo(userInfo)
          saleDetails += saleDetail
        }
      })

      jedisClient.close()
      saleDetails.toIterator
    })
    // 写入ES
//    saleDetailUserInfoDStream.foreachRDD( rdd => {
//
//      rdd.foreachPartition( iter => {
//
//        // 转换结构，添加doc_id
//        val result: Iterator[(String, SaleDetail)] = iter.map(saleDetail =>(s"${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))
//        MyEsUtil.insertBulk(GmallConstant.GMALL_SALE_DETAIL+sdfFormat.format(new Date(System.currentTimeMillis())), result.toList)
//      })
//    })


    //写入ES
    saleDetailUserInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //转换结构,添加doc_id
        val result: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (s"${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))
        MyEsUtil.insertBulk(
          GmallConstant.GMALL_SALE_DETAIL + sdfFormat.format(new Date(System.currentTimeMillis())),
          result.toList)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
