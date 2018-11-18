package com.erongda.bigdata.spark.logs

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用SparkCore对Apache Log进行分析
  */
object LogAnalyserSpark {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf对象
    val sparkConf = new SparkConf()
      .setAppName("LogAnalyserSpark").setMaster("local[2]")
    // 构建SparkContext实例对象
    val sc = SparkContext.getOrCreate(sparkConf)

    // 设置日志级别
    sc.setLogLevel("WARN")

    // TODO: 1. 读取数据
    val accessLogsRDD: RDD[ApacheAccessLog] = sc
      // 从本地文件系统读取
      .textFile(ContantUtils.LOCAL_DATA_DIC + "/access_log")
      // 过滤点不合格的数据
      .filter(log => ApacheAccessLog.isValidateLogLine(log))
      // 解析数据
      .map(ApacheAccessLog.parseLogLine)

    // 由于后续四个需求均对上述的RDD进行操作分析，可以将其缓存
    accessLogsRDD.persist(StorageLevel.MEMORY_AND_DISK_2)

    println(s"Count = ${accessLogsRDD.count()} \n ${accessLogsRDD.first()}")

    /**
      * 需求一：Content Size
      *     The average, min, and max content size of responses returned from the server
      */
    val contentSizeRDD: RDD[Long] =  accessLogsRDD.map(_.contentSize)
    // 此RDD使用多次，需要缓存
    contentSizeRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
    // compute
    val avgContentSize = contentSizeRDD.reduce(_ + _) / contentSizeRDD.count().toDouble
    val minContentSize = contentSizeRDD.min()
    val maxContentSize = contentSizeRDD.max()
    // 释放内存
    contentSizeRDD.unpersist()
    // 打印结果
    println(s"Content Size Avg: $avgContentSize, Min: $minContentSize, Max: $maxContentSize")


    /**
      * 需求二：Response Code
      *     A count of response code's returned.
      */
    val responseCodeToCount = accessLogsRDD
      // 类似词频统计WordCount
      .map(log => (log.responseCode, 1))
      // 聚合统计
      .reduceByKey(_ + _)
      // 返回数组
      .collect() // 由于Response Code 状态不多
    println(s"Response Code Count：${responseCodeToCount.mkString(", ")}")


    /**
      * 需求三：IP Address
      *     All IP Addresses that have accessed this server more than N times.
      */
    val ipAddresses = accessLogsRDD
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 30) // 访问网站的次数大于某个值
      .take(20)
    println(s"IP Addresses: ${ipAddresses.mkString(",")}")


    /**
      * 需求四：Endpoint
      *     The top endpoints requested by count.
      */
    val topEndpoints = accessLogsRDD
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      .top(5)(OrderingUtils.SencondValueOrdering)
    println(s"Top Endpoints: ${topEndpoints.mkString(",")}")

    // 释放缓存的内容
    accessLogsRDD.unpersist()

    // 为了WEB UI监控, 线程休眠
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }
}


object OrderingUtils{

  object SencondValueOrdering extends scala.math.Ordering[(String, Int)]{
    /**
      * 比较第二个Value的大小
      * @param x
      * @param y
      * @return
      */
    override def compare(x: (String, Int), y: (String, Int)): Int = x._2.compare(y._2)

  }


}
