package com.erongda.bigdata.spark.meituan.ad

/**
  * 封装广告点击的数据
  * @param timestamp
  *                  时间戳，毫秒级别时间戳，long类型
  * @param province
  *                 省份名称
  * @param city
  *             城市名称
  * @param userId
  *               用户ID
  * @param adId
  *             广告ID
  */
case class AdClickRecord(timestamp: Long, province: String,
                         city: String, userId: Int, adId: Int)
