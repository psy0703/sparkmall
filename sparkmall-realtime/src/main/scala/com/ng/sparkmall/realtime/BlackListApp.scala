package com.ng.sparkmall.realtime

import java.util

import com.ng.sparkmall.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉入黑名单。
  * 注意:
  * 1 我们把黑名单保存在 redis 中
  * 2 对已经进入黑名单的用户将不会再进行检查.
  */
object BlackListApp {

  //redis的一些相关参数
  val redisIp = "hadoop102"
  val redisPort = 6379
  val countKey = "user:day:adsclick"
  val blackListKey = "blacklist"

  //把用户写入到黑名单中
  def checkUserToBlackList(adsInfoDStream: DStream[AdsInfo]): Unit = {
    adsInfoDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition(adsInfoIt => {
          //建立redis连接
          val jedis = new Jedis(redisIp, redisPort)
          adsInfoIt.foreach(adsInfo => {
            //1. 在 redis 中计数. 使用 set Key: user:day:adsId   field: ...
            val countField = s"${adsInfo.userId}:${adsInfo.dayString}:${adsInfo.adsId}"
            //计数 + 1
            jedis.hincrBy(countKey, countField, 1)
            //2、达到阈值后写入黑名单
            if (jedis.hget(countKey, countField).toLong >= 100) {
              //如果点击某个广告的数量超过 100，将该用户加入黑名单
              jedis.sadd(blackListKey, adsInfo.userId)
            }
          })
          jedis.close()
        })
      }
    }

    // 过滤黑名单中的用户. 返回值是不包含黑名单的用户的广告点击记录的 DStream
    def checkUserFromBlackList(adsClickInfoDStream: DStream[AdsInfo], sc: SparkContext): Unit = {
      adsClickInfoDStream.transform(
        rdd => {
          val jedis = new Jedis(redisIp, redisPort)
          //读出黑名单
          val blackList: util.Set[String] = jedis.smembers(blackListKey)
          //将黑名单作为广播变量
          val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
          jedis.close()
          //将不在黑名单中的数据信息返回
          rdd.filter(
            adsInfo => {
              !blackListBC.value.contains(adsInfo.userId)
            }
          )
        })
    }
  }
}

/*
思路
思路
1、使用黑名单过滤 DStream, 得到新的不包含黑名单的用户的点击日志记录的新的 DStream

2、新的过滤后的 DStream 需要实时监测是否有新的用户需要加入到黑名单.
2.1在 redis中为每个userid_day_adsclick设置一个计数器(使用 hash 来存储数据)
2.2当计数器的值超过预定的阈值的时候, 加入黑名单(使用 set 集合来存储黑名单, 每个元素就是一个用户 id)
 */