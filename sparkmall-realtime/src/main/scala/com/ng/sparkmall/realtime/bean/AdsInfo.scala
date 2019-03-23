package com.ng.sparkmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 每个广告点击的数据类型
  * @param ts
  * @param area
  * @param city
  * @param userId
  * @param adsId
  */
case class AdsInfo (ts:Long,area:String,city:String,userId:String,adsId:String){

  val dayString = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
  override def toString: String = {
    s"$dayString:$area:$city:$adsId"
  }
}
