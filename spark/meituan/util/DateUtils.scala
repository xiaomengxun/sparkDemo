package com.erongda.bigdata.spark.meituan.util

import java.util.Calendar

import org.apache.commons.lang.time.FastDateFormat

/**
  * 时间相关的数据格式工具类
  */
object DateUtils {

  /**
    * 按照给定的毫秒级时间戳以及格式化字符串进行日期数据格式化
    * @param time
    *             Long类型时间格式
    * @param pattern
    *                格式化样式
    * @return
    */
  def parseLong2String(time: Long, pattern: String,
                       field: Int = 0, amount: Int = 0): String = {
    // 创建Calendar实例对象
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)

    if(0 != field && 0 != amount){
      cal.add(field, amount)
    }

    // 格式转换
    // new SimpleDateFormat(pattern).format(cal.getTime)
    FastDateFormat.getInstance(pattern).format(cal.getTime)
  }

}
