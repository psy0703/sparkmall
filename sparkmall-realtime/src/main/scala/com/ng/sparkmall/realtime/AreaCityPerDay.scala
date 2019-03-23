package com.ng.sparkmall.realtime

import com.ng.sparkmall.common.util.RedisUtil
import com.ng.sparkmall.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 需求6: 广告点击量实时统计
  * 每天各地区各城市各广告的点击流量实时统计
  */
object AreaCityPerDay {

  def statAreaCityAdsPerDay(filteredAdsInfoDStram:DStream[AdsInfo],sc:SparkContext): Unit ={

    //1、统计数据
    System.setProperty("HADOOP_USER_NAME","psy831")
    sc.setCheckpointDir("hdfs://hadoop102:9000//checkpoint")

    val resultDStream: DStream[(String, Long)] = filteredAdsInfoDStram.map {
      adsInfo => {
        (s"${adsInfo.dayString}:${adsInfo.area}:${adsInfo.city}:${adsInfo.adsId}", 1L)
      }
    }.reduceByKey(_ + _).updateStateByKey(
      (seq: Seq[Long], opt: Option[Long]) => Some(seq.sum + opt.getOrElse(0L))
    )

    //2、写入到Redis
    resultDStream.foreachRDD(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      //将数据拉去到Driver端
      val totalCountArr: Array[(String, Long)] = rdd.collect
      //写入redis
      totalCountArr.foreach{
        case (field,count) => client.hset("day:area:city:adsCount",field,count.toString)
      }
      client.close()
    })

  }
}
/*
思路
结果保存在 redis 中, 值的类型为 hash.

key ->  "date:area:city:ads"
kv -> field:   2018-11-26:华北:北京:5     value: 12001
rdd[AdsInfo] => rdd[date:area:city:ads, 1] => reduceByKey(_+_)

=> updaeStateByKey(...)

设置 checkpoint
 */
/*
如果checkpoint的目录想要设置在hdfs上, 则一定要在创建SparkConf前设置HADOOP_USER_HOME: System.setProperty("HADOOP_USER_NAME", "atguigu"). 否则会出现权限不足的情况.
 */