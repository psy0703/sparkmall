package com.ng.sparkmall.offline.app

import java.util.Properties

import com.ng.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.sql.SparkSession

/**
  * 需求4: 各区域热门商品 Top3
  * 计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示
  */
object AreaClickTop3App {

  def statAreaClickTop3Product(spark: SparkSession): Unit = {
    // 注册 UDAF 函数
    spark.udf.register("city_remark", CityClickCountUDAF)

    // 1. 查询出来所有的点击记录, 并与 city_info 表连接, 得到每个城市所在的地区
    spark.sql("use sparkmall")
    spark.sql(
      """
        |select
        |c.area,
        |c.city_id,
        |c.city_name,
        |u.click_product_id,
        |u.click_category_id
        |from city_info c
        |join user_visit_action u
        |on c.city_id = u.city_id
        |where u.click_product_id != -1
      """.stripMargin).createOrReplaceTempView("t1")

    //2、按照地区和商品id进行分组，统计每个商品的总点击次数
    spark.sql(
      """
        |select
        |t1.area , t1.click_product_id,
        |count(*) click_count,
        |city_remark(t1.city_name) remark
        |from t1
        |group by t1.area , t1.click_product_id
      """.stripMargin).createOrReplaceTempView("t2")

    //3、 每个地区内按照点击次数降序排列
    spark.sql(
      """
        |select
        |t2.*,
        |rank() over(partition by t2.area order by t2.click_count desc) ranks
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")


    //4、只取前三名. 并把结果保存在数据库中
    val conf = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user", conf.getString("jdbc.user"))
    props.setProperty("password", conf.getString("jdbc.password"))

    spark.sql(
      """
        |select
        |t3.area,
        |t3.click_count,
        |t3.remark,
        |t3.ranks
        |from t3
        |where t3.ranks <=3
      """.stripMargin).show()
//      .write
//      .mode(SaveMode.Overwrite)
//      .jdbc(conf.getString("jdbc.url"), "area_click_top10", props)
  }

}
