package com.erongda.bigdata.spark.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用SparkCore程序实现分组、排序和TopKey, 使用combineByKey进行聚合
  */
object SparkGroupCombiner {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("SparkGroupSort")
      .setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(config)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")


    // TODO: 1. 读取数据
    val inputRDD = sc.textFile(s"${ContantUtils.LOCAL_DATA_DIC}/group.data")

    /**
      * 每行数据的字段信息：
      *     aa 78
      *     包含两个字段，各个字段使用 空格 隔开
      */
    val sortTopRDD = inputRDD
      // a. 转换数据类型，变为二元组
      .map(line => {
        // 分割单词
        val splited = line.split("\\s+")
        // 返回
        (splited(0), splited(1).toInt)
      })
      // 使用combineByKey聚合
      .combineByKey(
        // 第一个参数：createCombiner: V => C
        (v: Int) => ListBuffer[Int](v),
        // 第二个参数：mergeValue: (C, V) => C
        (c: ListBuffer[Int], v: Int) => {
          c += v
          c.sortBy(- _).take(3)
        },
        // 第三个参数： mergeCombiners: (C, C) => C
        (c1: ListBuffer[Int], c2: ListBuffer[Int]) => {
          c1 ++= c2
          c1.sortBy(- _).take(3)
        }
      )
      /*
        def combineByKey[C](
          // 针对每个分区中Value的聚合初始化，聚合中间临时变量值的初始化
          createCombiner: V => C,
          // 针对每个分区中的Value进行合并
          mergeValue: (C, V) => C,
          // 针对各个分区聚合的结果进行聚合
          mergeCombiners: (C, C) => C
        ): RDD[(K, C)]
       */

    sortTopRDD.coalesce(1).foreachPartition(_.foreach(println))

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
