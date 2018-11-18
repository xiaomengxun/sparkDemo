package com.erongda.bigdata.spark.core

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Kian on 2018/10/30.
  */
object AccumalatorTest {

  def main(args: Array[String]): Unit = {

    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("AccumalatorTest")
      .setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(config)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    // 定义一个Long累加器
    val accum: LongAccumulator = sc.longAccumulator("My Accumulator")

    // TODO: 1. 读取数据
    val inputRDD = sc.textFile("/datas/wordcount.data", minPartitions = 2)

    val countVal = inputRDD.flatMap(line => {
      // 有一条数据就加1L
      accum.add(1L)

      line.split("\\s+")
    }).count()

    println(s"Count = $countVal, accum = ${accum.value}, Lines = ${inputRDD.count()}")


    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
