package com.ng.sparkmall.realtime

import com.ng.sparkmall.common.util.MyKafkaUtil
import com.ng.sparkmall.realtime.bean.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val conf: SparkConf = new SparkConf().setAppName("RealTimeApp ")
      .setMaster("local[*]")
    // 2. 创建 SparkContext 对象
    val sc = new SparkContext(conf)
    // 3. 创建 StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    //4、得到DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc,"ads_log")

    //5、为了方便后面的计算，把消费到的字符串封装到到对象中
    recordDStream.map{
      record => {
        val split: Array[String] = record.value.split(",")
        AdsInfo(split(0).toLong,split(1),split(2),split(3),split(4))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
