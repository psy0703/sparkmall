package com.ng.sparkmall.common.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * RedisUtil工具类
  */
object RedisUtil {

  private val config = ConfigurationUtil("config.properties")
  private val host: String = jedisPoolConfig.getString("redis.host")
 private val port: Int = jedisPoolConfig.getInt("redis.port")

  //创建redis连接池
  private val jedisPoolConfig = new JedisPoolConfig()
  //最大连接数
  jedisPoolConfig.setMaxTotal(100)
  //最大空闲
  jedisPoolConfig.setMaxIdle(20)
  //最小空闲
  jedisPoolConfig.setMinIdle(20)
  //忙碌时是否等待
  jedisPoolConfig.setBlockWhenExhausted(true)
  //忙碌时的等待时长 毫秒
  jedisPoolConfig.setMaxWaitMillis(500)
  //每次获得连接是否进行测试
  jedisPoolConfig.setTestOnBorrow(true)

  private val jedisPool = new JedisPool(jedisPoolConfig, host, port)
  //直接得到一个redis的连接
  val getJedisClient: Jedis = jedisPool.getResource

}
