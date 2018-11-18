package com.erongda.bigdata.spark.core

import com.erongda.bigdata.spark.core.OrderingUtils.TupleOrdering
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现从HDFS读取数据，进行词频统计WordCount，实现TOP
  */
object SparkWordCountTop {

  /**
    * Scala 语言中程序的入口
    * @param args
    *             程序运行传递的参数
    */
  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("SparkWordCountTop")
      .setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(config)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    /**
      * 第一步、读取数据，从HDFS读取数据
      */
    val inputRDD: RDD[String] = sc.textFile("/datas/wordcount.data")

    // 样本数据和条目数
    println(s"Count = ${inputRDD.count()}")
    println(s"First: \n\t${inputRDD.first()}")

    /**
      * 第二步、数据的分析（调用RDD中转换函数）
      */
    val wordCountRDD: RDD[(String, Int)] = inputRDD
      // 对每一条数据进行分割为单词
      .flatMap(line => line.split("\\s+"))
      // 将单词转换为二元组形式
      .map(word => (word, 1))
      // 按照Key（单词）分组，聚合统计出现的总次数
      .reduceByKey((a, b) => a + b)

    // TODO: 对统计词频进行排序（降序），获取TopKey（Key=3）-> 词频出现次数最多的三个单词
    wordCountRDD
      .coalesce(1)
      .map(_.swap)
      // def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      .sortByKey(ascending = false)
      .foreach(println)

    println("========================================")

    wordCountRDD
      .map(_.swap)
      // def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
      .takeOrdered(3)
      .foreach(println)

    println("========================================")

    // def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    wordCountRDD.map(_.swap).top(3).foreach(println)

    println("========================================")
    wordCountRDD.top(3)(TupleOrdering).foreach(println)

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }
}


/**
  * 创建object对象工具类，专门自定义排序规则
  *
  * TODO： 在Scala中object对象就是相当于一个类class的单实例对象
  */
object OrderingUtils{

  /**
    * 针对二元组类型的数据进行排序
    */
  object TupleOrdering extends scala.math.Ordering[(String, Int)]{
    // 针对compare来说，如果返回值 大于等于1 表示降序；如果返回值小于等于1表示升序；如果是0，表示相等
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      x._2 - y._2
    }
  }



}