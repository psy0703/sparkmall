package com.ng.sparkmall.offline.app

import com.ng.sparkmall.common.bean.UserVisitAction
import com.ng.sparkmall.common.util.JDBCUtil
import com.ng.sparkmall.offline.acc.MapAccumulator
import com.ng.sparkmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/*
1. 遍历全部日志表, 根据品类 id 和操作类型分别累加. 需要用到累加器

    - 定义累加器
        累加器用什么来保存? map
         key:  (categoryId, action)    元组来表示
         value: count
    - 当碰到订单和支付业务的时候注意拆分字段才能得到品类 id

2. 遍历完成之后就得到每个每个品类 id 和操作类型的数量. 然后按照 cid 进行聚合, 聚合成 CategoryCountInfo 类型

3. 按照 点击 下单 支付 的顺序来排序

4. 取出 Top10

5. 写入到 Mysql 数据库
*/
object CategoryTop10App {

  //统计热门品类的Top10
  def statCategoryTop10(spark:SparkSession,userVisitActionRDD: RDD[UserVisitAction], taskId : String): Unit ={
    //1、注册累加器
    val acc = new MapAccumulator
    spark.sparkContext.register(acc,"CategoryActionAcc")

    //2、遍历日志
    userVisitActionRDD.foreach{
      visitAction => {
        if(visitAction.click_category_id == -1){
          acc.add(visitAction.click_category_id.toString,"click")
        } else if(visitAction.order_category_ids != null){
          visitAction.order_category_ids.split(",").foreach{
            old => acc.add(old,"order")
          }
        } else if(visitAction.pay_category_ids != null){
          visitAction.pay_category_ids.split(",").foreach{
            pid => acc.add(pid,"pay")
          }
        }
      }
    }

    //3、遍历完成后就得到每个品类id和操作类型的数量，然后按照CategoryId进行分组
    val actionCountByCategoryIdMap: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

    //4、聚合成CategoryCountInfo类型的集合
    val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCategoryIdMap.map {
      case (cid, actionMap) => CategoryCountInfo(
        taskId,
        cid,
        actionMap.getOrElse((cid, "click"), 0),
        actionMap.getOrElse((cid, "order"), 0),
        actionMap.getOrElse((cid, "pay"), 0)
      )
    }.toList

    //5 、 按照 点击 、下单 、 支付 的顺序 按降序排序

    val sortedCategoryInfoList: List[CategoryCountInfo] = categoryCountInfoList.sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse))

    //6、截取前10
    val top10: List[CategoryCountInfo] = sortedCategoryInfoList.take(10)

    //7、插入数据库
    val argsList: List[Array[Any]] = top10.map(info => Array(info.taskId, info.categoryId, info.clickCount, info.orderCount, info.payCount))

    JDBCUtil.executeBatchUpdate("insert into table category_top10 values(?,?,?,?,?",argsList)
  }
}

























