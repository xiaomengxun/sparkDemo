package com.erongda.bigdata.spark.meituan.util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 专门解析程序传递参数工具类
  */
object ParamUtils {

  /**
    * 从命令行参数中提取TaskId
    * @param args
    *             程序参数
    * @return
    */
  def getTaskIdFromArgs(args: Array[String]): Long = {
    var taskId: Long = -1
    try{
      if(args != null && args.length > 0) taskId = args(0).toLong
    }catch {
      case e: Exception => e.printStackTrace()
    }
    taskId
  }

  /**
    * 从JSON对象中提取参数的值
    * @param jsonObejct
    *                   JSON对象
    * @param field
    *              字段名称
    * @return
    */
  def getParam(jsonObejct: JSONObject, field: String): Option[String] = {
    // 依据字段名称获取值
    val value = jsonObejct.getString(field)
    // 判断
    if(null == value || value.trim.isEmpty) Option.apply(null) else Option.apply(value)
  }

  /**
    * 解析JSON格式数据
    * @param json
    *             JSON字符串
    * @return
    */
  def getTaskParam(json: String): JSONObject = {
    // 解析JSON字符串
    val taskParam = JSON.parseObject(json)
    // 判断并返回
    if(null != taskParam && !taskParam.isEmpty) taskParam else null
  }

}
