package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  private val sdfformat = new SimpleDateFormat("yyyy-MM-dd")

  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {

    val dateMidLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(log =>(s"${log.logDate}_${log.mid}", log))

    val dateMidToIterDStrem: DStream[(String, Iterable[StartUpLog])] = dateMidLogDStream.groupByKey()

    val flatDStream: DStream[StartUpLog] = dateMidToIterDStrem.flatMap {
      case (_, logIter) => {
        logIter.toList.sortWith(_.ts < _.ts).take(1)
      }
    }
    flatDStream
  }

  /**
    * 根据Redisz中的数据进行跨批次进行去重
    * @param startDStream
    */
  def filterByRedis(ssc: StreamingContext,startDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    // transform 有返回值，foreachRdd返回值unit
    /*startDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        // 获取连接
        val redisClient = RedisUtil.getJedisClient
        // 对每条数据进行过滤
        val logs: Iterator[StartUpLog] = iter.filter { log =>
          val redisKey = s"dau:${log.logDate}"
          !redisClient.sismember(redisKey, log.mid)
        }
        redisClient.close()
        logs
      })
      rdd
    })*/

    // 方案二
    startDStream.transform(rdd => {

      println(s"过滤前有 ${rdd.count()} 条数据")

      // 每个批次执行一次，用广播变量
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val times = System.currentTimeMillis()
      val dateStr: String = sdfformat.format(new Date(times))

      println(">>>>>>>>>>>>>>>>>>>"+dateStr)

      val mids: util.Set[String] = jedisClient.smembers(s"dau:$dateStr")

      val midsBc: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      jedisClient.close()
      val filRddValue: RDD[StartUpLog] = rdd.filter(log => !midsBc.value.contains(log.mid))

      println(s"第一次过滤后有 ${filRddValue.count()} 条数据")
      filRddValue
    })
  }

  /**
    * 将两次过滤之后的Mid写入redis
    *
    * @param startDStream
    */
  def saveMidToRedis(startDStream: DStream[StartUpLog]) = {

    startDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val redisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(log => {
          val redisKey: String = s"dau:${log.logDate}"
          println(">>>>>>>redisKey>>>>>>>>>>>>>" +redisKey)
          redisClient.sadd(redisKey, log.mid)
        })
        redisClient.close()
      })
    })
  }
}
