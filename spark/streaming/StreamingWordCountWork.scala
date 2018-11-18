package com.erongda.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 从Socket中读物数据，分析处理，将结果打印到 控制台上， 实时接收数据，统计每批次数据中词频WordCount
  */
object StreamingWordCountWork {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("StreamingWordCountWork")
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")

    /**
      * TODO: 1. 从数据源实时接收流式数据
      *     一直接收数据
      *       def socketTextStream(
                  hostname: String,
                  port: Int,
                  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                ): ReceiverInputDStream[String]
      */
    val inputDStream1: DStream[String] = ssc.socketTextStream("bigdata-training01.erongda.com", 9998)
    val inputDStream2: DStream[String] = ssc.socketTextStream("bigdata-training01.erongda.com", 9999)

    val inputDStream: DStream[String] = inputDStream1.union(inputDStream2)

    /**
      * TODO：2. 调用DStream中API对数据分析处理
      *     每批次处理数据: 针对每批次数据进行词频统计（此处每次统计5秒内的数据）
      */
    /**
      * def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
      *   此函数 针对 每批次RDD 数据进行直接的转换操作
      * TODO：
      *   在SparkStreaming企业编程开发中
      *     建议：如果能使用transform函数针对RDD操作完成的，就不使用DStream中其他转换函数
      */
    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {
      rdd
        // a. 分割单词
        .flatMap(line => line.split("\\s").map(_.trim).filter(_.length > 0))
        // b. 转换为二元组
        .map(word => (word, 1))
        // c. 按照Key聚合统计
        .reduceByKey((a, b) => a + b)
    })


    /**
      * TODO：3. 将每批次处理的结果进行输出
      *     每批次的结果输出
      *       def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
      */
    wordCountDStream.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      println("-----------------------------------------")
      val batchTime = time.milliseconds
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val batchDateTime = sdf.format(new Date(batchTime))
      println(s"Batch Time: $batchDateTime")
      println("-----------------------------------------")
      if(!rdd.isEmpty()){
        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
    })


    // TODO: 4. 启动实时流式应用，开始准备接收数据并进行处理
    ssc.start() // 将会启动Receiver接收器，用于接收数据源端的数据
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
