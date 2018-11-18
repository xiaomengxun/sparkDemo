package com.erongda.bigdata.spark.meituan.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 自定义UDAD，针对各个区域不同城市中商品的点击次数的统计
  *
  * 分析聚合函数功能及过程： 针对<海底捞> 商品被点击次数
  *     -> 华东 :   ->  3 + 2 + 4 = 9
  *           上海 ->  sh-click-01   sh-click-02   sh-click-03
  *           苏州 ->  sz-click-01   sz-click-02
  *           南京 ->  nj-click-01   nj-click-01   nj-click-01  nj-click-01
  *           聚合结果：
  *               上海:3;苏州:2;南京:4
  *     -> 华北 ：  -> 2 + 1 = 3
  *           北京 ->  bj-click-01   bj-click-02
  *           西安 ->  xa-click-01
  *           聚合结果：
  *               北京:2;西安:1
  *   聚合函数的关键点：
  *       - 聚合过程中 中间临时缓冲值（临时变量）
  *           bufferStr
  *           如何聚合操作,如何进行的？？
  *           -a. bufferStr是否有值
  *             无值，直接赋值，此城市被点击一次   count = 1
  *           -b. bufferStr 有值
  *             是否包含当前城市
  *             - b.1 如果不包含，直接在字符串后期追加当前城市统计信息
  *                 此时当前城市被点击一次  count = 1
  *             - b.2 包含当前城市
  *                 缓冲值的分割：
  *                     第一次分割，按照分号分割，得到每个城市被点击次数信息
  *                     判断 分割以后 城市是否与当前城市相同，如果相同
  *                         第二次分个，获取当前城市以前被点击的次数，然后加1，得到最新被点击的次数
  */
object GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

  val delimiter: String = ","

  /**
    * 给定输入数据的类型
    * @return
    */
  override def inputSchema: StructType = StructType(
    StructField("str", StringType, nullable = true) :: Nil
  )


  /**
    * 数据聚合过程中，临时缓冲数据类型
    * @return
    */
  override def bufferSchema: StructType =  StructType(
    StructField("bufferStr", StringType, nullable = true) :: Nil
  )

  /**
    * 聚合函数返回的数据类型，此处是字符串
    * @return
    */
  override def dataType: DataType = StringType

  /**
    * 表示的是唯一性，是否支持模型查询（多次查询返回结果是否一致）：true表示不支持，返回结果一致；false相反
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 对各个分区每次开始聚合时，临时缓冲数据的初始化
    * @param buffer
    *               聚合中中间的临时缓冲值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "") // 初始化空字符串
  }

  /**
    * 针对每个分区中数据的聚合操作，表示每个分区中数据的聚合
    * @param buffer
    *               聚合中中间临时缓冲值
    * @param input
    *              聚合中每次输入的数据
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取 缓存区中的值
    val bufferValue = buffer.getString(0)

    // 获取传递进来的值
    val inputValue = input.getString(0)

    // 合并数据
    val mergeValue = mergeValueFunc(bufferValue, inputValue)

    // 更新缓存区中的字
    buffer.update(0, mergeValue)
  }

  /**
    * 合并两个缓冲区的值，该方法只有在多个分区聚合数据在合并的数据才会使用
    * @param buffer1
    *                各个分区聚合的临时缓冲值，可变的，最终更新合并结果到此缓冲值中
    * @param buffer2
    *                分区聚合的临时缓冲值，不可变的
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 分别获取缓冲区 中的值
    var buffer1Value = buffer1.getString(0)
    val buffer2Value = buffer2.getString(0)

    // 合并数据
    for(tmpStr <- buffer2Value.split(delimiter)){
      // tmpStr的格式类似：“101:上海:25”
      val arr = tmpStr.split(":")
      buffer1Value = mergeValueFunc(buffer1Value, s"${arr(0)}:${arr(1)}", count = arr(2).toInt)
    }

    // 更新缓冲区数据
    buffer1.update(0, buffer1Value)
  }

  /**
    * 最终聚合函数的返回
    * @param buffer
    *               最终聚合的临时缓冲值
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getString(0)

  /**
    * 将value合并到buffer中，同时注意数据上去重操作
    * @param buffer
    *               缓存数据，格式类型："101:上海:25;102:杭州:30"
    *               使用","分割不同城市点击商品的次数统计，使用":"分割的城市点击数据信息
    * @param value
    *              需要合并的数据，格式类似："101:上海“，以":"进行数据分割
    * @param count
    *              value 对应出现的次数
    * @return
    */
  private def mergeValueFunc(buffer: String, value: String, count: Int = 1): String ={
    // a. bufferStr是否有值
    if(buffer.contains(value)){
      // 更新操作
      buffer.split(delimiter).map(v => {
        // 数据v的格式：“101:上海:25”
        if(v.contains(value)){
          // 更新v中的值
          s"$value:${v.split(":")(2).toInt + count}"
        }else{
          v
        }
      }).mkString(delimiter)
    }else{
      // 插入操作：如果buffer为空，直接返回value；否则添加一个分割符后，再返回追加后的结果
      if("".equals(buffer)){
        s"$value:$count"
      }else{
        s"$buffer$delimiter$value:$count"
      }
    }
  }
}
