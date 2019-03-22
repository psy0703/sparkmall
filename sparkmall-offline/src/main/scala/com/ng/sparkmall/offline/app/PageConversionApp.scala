package com.ng.sparkmall.offline.app

import java.text.DecimalFormat

import com.ng.sparkmall.common.bean.UserVisitAction
import com.ng.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 需求三：页面单条转化率统计
  * 1. 读取到规定的页面
  * 例如: targetPageFlow:"1,2,3,4,5,6,7"
  *
  * 2. 过滤出来规定页面的日志记录 并统计出来每个页面的访问次数
  * 例如: 只需过滤出来1,2,3,4,5,6   第7页面不需要过滤
  *
  * 3. 计算页面跳转次数(肯定是按照每个 session 来统计)
  * 1->2  2->3 ...
  *     3.1 统计每个页面访问次数
  *
  * 4. 计算转化率
  * 页面跳转次数 / 页面访问次数
  * 1->2/1 表示页面1到页面2的转化率
  *
  * 5. 保存到数据库
  *
  */
object PageConversionApp {

  def calcPageConversion(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],targetPageFlow:String,taskId:String):Unit={

    //1、读取到规定的页面
    val pageFlowArr: Array[String] = targetPageFlow.split(",")
    val prePageFlowArr: Array[String] = pageFlowArr.slice(0,pageFlowArr.length - 1)
    val postPageFlowArr: Array[String] = pageFlowArr.slice(1,pageFlowArr.length)
    //2、明确哪些页面需要计算跳转次数 1->2  2->3 3->4 ...
    val targetJumpPages: Array[String] = prePageFlowArr.zip(postPageFlowArr).map( t => t._1 + "->" + t._2)

    //3、用userVisitActionRDD过滤出规定页面的日志记录，并统计出每个页面的访问次数  countByKey 是行动算子  reduceByKey是转换算子
    val targetPageCount: collection.Map[Long, Long] = userVisitActionRDD.filter(action => pageFlowArr.contains(action.page_id.toString))
      .map(action => (action.page_id, 1L)).countByKey

    targetPageCount.foreach(println)

    //4、按照session_id分组统计所有页面的跳转次数：先按session_id分组，再按action_time升序排序
    //4.1 按照 session 分组, 然后并对每组内的 UserVisitAction 进行排序
    val pageJumpRDD: RDD[String] = userVisitActionRDD.groupBy(_.session_id).flatMap {
      case (sid, actions) => {
        val visitActions: List[UserVisitAction] = actions.toList.sortBy(_.action_time)
        //4.2 转换访问流水
        val pre: List[UserVisitAction] = visitActions.slice(0, visitActions.length - 1)
        val post: List[UserVisitAction] = visitActions.slice(1, visitActions.length)
        //4.3 过滤出来和统计目标一致的跳转信息
        pre.zip(post).map(item => item._1.page_id + "->" + item._2.page_id).filter(targetJumpPages.contains(_))
      }
    }

    //5、统计跳转次数，由于数据量已经很小了，拉到Driver端进行计算
    val pageJumpCount: collection.Map[String, Long] = pageJumpRDD.map((_, 1L)).countByKey()

    println("---------")
    pageJumpCount.foreach(println)


    //6、计算跳转率
    val formatter = new DecimalFormat(".00%")
    //转换成百分比
    val conversionRate: collection.Map[String, Any] = pageJumpCount.map {
      case (p2p, jumpCount) => {
        val key: Long = p2p.split("->")(0).toLong
        val visitCount: Long = targetPageCount.getOrElse(key, 0L)
        val rate: Double = jumpCount.toDouble / visitCount
        (p2p, formatter.format(rate))
      }
    }
    conversionRate.toList.sortBy(_._1.charAt(0))
    conversionRate.foreach(println)

    // 7. 存储到数据库
    val result: Iterable[Array[Any]] = conversionRate.map {
      case (p2p, conver) => Array(taskId, p2p, conver)
    }

    JDBCUtil.executeUpdate("truncate table sparkmall.page_conversion_rate", null)
    JDBCUtil.executeBatchUpdate("insert into sparkmall.page_conversion_rate values(?, ?, ?)", result)
  }
}
