package com.erongda.bigdata.spark.meituan.ad

/**
  * 封装广告点击统计的维度信息
  * @param date
  *             日期，格式为：yyyyMMdd
  * @param province
  *                 省份名称
  * @param city
  *             城市名称
  * @param adId
  *             广告ID
  */
case class StateDimension(date: String,
                          province: String, city: String, adId: Int)
