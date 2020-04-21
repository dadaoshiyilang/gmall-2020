package com.atguigu.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  var jedisPool:JedisPool = _

  def getJedisClient:Jedis = {

    if (jedisPool == null) {
      val config: Properties = PropertiesUtil.load("config.properties")

      val host:String = config.getProperty("redis.host")
      val port:String = config.getProperty("redis.port")

      val jedisConfig: JedisPoolConfig = new JedisPoolConfig()
      jedisConfig.setMaxTotal(100)// 最大连接数
      jedisConfig.setMaxIdle(20)// 最大空闲
      jedisConfig.setMinIdle(20)// 最小空闲
      jedisConfig.setBlockWhenExhausted(true) // 忙碌时是否等待
      jedisConfig.setMaxWaitMillis(500) // 忙碌时等待时长 毫秒
      jedisConfig.setTestOnBorrow(true) // 每次获得连接的进行测试

      jedisPool= new JedisPool(jedisConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
}
