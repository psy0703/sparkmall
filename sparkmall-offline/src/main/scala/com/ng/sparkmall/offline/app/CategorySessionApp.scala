package com.ng.sparkmall.offline.app

import com.ng.sparkmall.common.bean.UserVisitAction
import com.ng.sparkmall.common.util.JDBCUtil
import com.ng.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {

  def statCategoryTop10Seesion(spark: SparkSession, categoryTop10: List[CategoryCountInfo], userVisitAction: RDD[UserVisitAction], taskId: String): Unit = {
    //1、过滤掉userVisitAction中category 不再前10 的日志
    //1.1 得到top10 的categoryId
    val categoryIdTop10: List[String] = categoryTop10.map(_.categoryId)
    //广播变量
    val categoryIdTop10BD: Broadcast[List[String]] = spark.sparkContext.broadcast(categoryIdTop10)
    //1.2 过滤出来过滤掉userVisitAction中categorytop10 的日志
    val filteredActionRDD: RDD[UserVisitAction] = userVisitAction.filter(
      info => categoryIdTop10BD.value.contains(info.click_category_id.toString)
    )

    //2 转换结果为RDD[(categoryId,sessioinId),1],然后统计结果
    val categorySessionCountRDD: RDD[((Long, String), Int)] = filteredActionRDD.map(userAction =>
      ((userAction.click_category_id, userAction.session_id), 1)
    ).reduceByKey(_ + _)

    //3、统计每个品类top10 =》 RDD[categoryId,(SessionId,count)] => RDD[categoryId,Iterable[(sessionId,count)]]
    val categorySessionGrouped: RDD[(Long, Iterable[(String, Int)])] = categorySessionCountRDD.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }.groupByKey()

    //4、对每个Iterable[(sessionId,count)]进行排序，并取每个Iterable的前10
    //5、吧数据封装到CategorySession中
    val sortedCategorySessioin: RDD[CategorySession] = categorySessionGrouped.flatMap {
      case (cid, it) => {
        it.toList.sortBy(_._2)(Ordering.Int.reverse)
          .take(10)
          .map {
            item => CategorySession(taskId, cid.toString, item._1, item._2)
          }
      }
    }

    //6、写入到mysql数据库   数据量已经很小了，可以把数据拉到Driver，然后再写入
    val categorySessionArr: Array[Array[Any]] = sortedCategorySessioin.collect.map(
      item => Array(item.taskId, item.categoryId, item.sessionId, item.clickCount)
    )
    JDBCUtil.executeUpdate("truncate table sparkmall.category_top10_session_count", null)
    JDBCUtil.executeBatchUpdate("insert into sparkmall.category_top10_session_count values(?,?,?,?)", categorySessionArr)
  }

}
