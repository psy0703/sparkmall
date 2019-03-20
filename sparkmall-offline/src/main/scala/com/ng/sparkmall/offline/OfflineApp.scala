package com.ng.sparkmall.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.ng.sparkmall.common.bean.UserVisitAction
import com.ng.sparkmall.common.util.ConfigurationUtil
import com.ng.sparkmall.offline.bean.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.ng.sparkmall.offline.app._

object OfflineApp {

  def main(args: Array[String]): Unit = {
    //创建Spark Session对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Offline")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()

    val taskId: String = UUID.randomUUID().toString

    //根据条件过滤取出需要的RDD，过滤条件定义在配置文件中
    val value: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readConditions)
    value.take(10).foreach(println)

//    CategoryTop10App.statCategoryTop10(spark,value,taskId)
  }

  /**
    * 读取过滤条件
    *
    * @return
    */
  def readConditions: Condition = {
    //读取配置文件
    val config = ConfigurationUtil("conditions.properties")
    //读取到其他的JSON字符串
    val conditionString: String = config.getString("condition.params.json")
    //解析成Condition对象
    JSON.parseObject(conditionString, classOf[Condition])
  }

  /**
    * 读取指定条件的 UserVisitActionRDD
    *
    * @param spark
    * @param condition
    */
  def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
    var sql = s"select v.* from user_visit_action v join user_info u on v.user_id = u.user_id where 1=1"
    if(isNotEmpty(condition.startDate)){
      sql += s" and v.date >= '${condition.startDate}'"
    }
    if(isNotEmpty(condition.endDate)){
      sql += s" and v.date <= '${condition.endDate}' "
    }
    if (condition.startAge != 0){
      sql += s" and u.age >= '${condition.startAge}' "
    }
    if (condition.endAge != 0 ){
      sql += s" and u.age <= '${condition.endAge}' "
    }

    import spark.implicits._
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd
  }

}
