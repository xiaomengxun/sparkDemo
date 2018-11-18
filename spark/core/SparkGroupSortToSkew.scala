package com.erongda.bigdata.spark.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 使用SparkCore程序实现分组、排序和TopKey，使用分阶段聚合（分为两个阶段）
  */
object SparkGroupSortToSkew {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("SparkGroupSortToSkew")
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
    val sortTopRDD: RDD[(String, List[Int])] = inputRDD
      // a. 转换数据类型，变为二元组
      .map(line => {
        // 分割单词
        val splited = line.split("\\s+")
        // 产生一个随机数
        val random = new Random()
        // 返回
        (random.nextInt(2) + "_" + splited(0), splited(1).toInt)
      })
      // TODO: 第一阶段局部聚合操作
      // b. 按照Key进行分组
      .groupByKey()  // RDD[(String, Iterable[Int])]
      // c. 分组以后，对每个组内的数据进行降序排序，获取前3个值
      .map{
        case (key: String, iter: Iterable[Int]) =>
          // 此时的核心，针对组内排序
          val sortedList = iter.toList.sorted.takeRight(3).reverse
          // 返回
          (key.split("_")(1), sortedList)
      }
      // TODO: 第二阶段全局聚合操作
      .groupByKey()  // RDD[(String, Iterable[List[Int]])]
      .map{ case (key: String, iter: Iterable[List[Int]]) =>
        (key, iter.toList.flatten.sorted.takeRight(3).reverse)
      }

    sortTopRDD.coalesce(1).foreachPartition(_.foreach(println))

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
