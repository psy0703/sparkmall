package com.ng.sparkmall.common.util

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * 定义MyKafkaUtil, 用来从 Kafka 读取数据得到 DStream
  */
object MyKafkaUtil {

  val config = ConfigurationUtil("config.properties")
  val broker_list: String = config.getString("kafka.broker.list")

  //kafka消费者配置
  private val kafkaParam = Map(
    //用于初始化链接到集群的地址
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group_id" -> "commerce-consumer-group",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量. 本次我们仍然自动维护偏移量
    "enable.auto.commit" -> (true:lang.Boolean)
  )
  /*
     创建DStream，返回接收到的输入数据
     LocationStrategies：根据给定的主题和集群地址创建consumer
     LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
     ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
     ConsumerStrategies.Subscribe：订阅一系列主题
     */
  def getDStream(ssc:StreamingContext,topic:String):InputDStream[ConsumerRecord[String, String]]={
    val DStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 标配. 只要 kafka 和 spark没有部署在一台设备就应该是这个参数
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    return DStream
  }
}
