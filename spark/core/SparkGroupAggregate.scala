package com.erongda.bigdata.spark.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用SparkCore程序实现分组、排序和TopKey, 使用aggregateByKey进行聚合
  */
object SparkGroupAggregate {

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
      // def aggregateByKey(zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]
      // TODO: topN -> key: List(N)
      .aggregateByKey(ListBuffer[Int]())(
        // 第一个参数：seqOp: (U, V) => U  针对每个分区中Value数据进行“聚合”
        (u, v) => {
          // 将每个元素加入到ListBuffer中
          u += v
          // 对ListBuffer中数据进行降序排序，获取TopN
          u.sorted.takeRight(3).reverse
        },
        // 第二参数：combOp: (U, U) => U  将所有分去的聚合结果合并在一起
        (u1, u2) => {
          // 合并两个分区的聚合结果ListBuffer
          u1 ++= u2
          // 对ListBuffer中数据进行降序排序，获取TopN
          u1.sorted.takeRight(3).reverse
        }
      )


    sortTopRDD.coalesce(1).foreachPartition(_.foreach(println))

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
