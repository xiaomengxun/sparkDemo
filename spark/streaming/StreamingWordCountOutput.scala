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
object StreamingWordCountOutput {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("StreamingWordCount")
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // val ssc = new StreamingContext(sc, Seconds(5))

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
    val inputDStream: DStream[String] = ssc.socketTextStream("bigdata-training01.erongda.com", 9999)


    /**
      * TODO：2. 调用DStream中API对数据分析处理
      *     每批次处理数据: 针对每批次数据进行词频统计（此处每次统计5秒内的数据）
      */
    // a. 分割单词
    val wordDStream = inputDStream.flatMap(line => line.split("\\s+"))

    // b. 转换为二元组
    val tupleDStream = wordDStream.map(word => (word, 1))

    // c. 按照Key聚合统计
    val wordCountDStream = tupleDStream.reduceByKey((a, b) => a + b)


    /**
      * TODO：3. 将每批次处理的结果进行输出
      *     每批次的结果输出
      */
    // wordCountDStream.print(10)
/*
    wordCountDStream.foreachRDD(rdd => {
      // TODO：判断结果RDD是否有数据，没有的话， 不进行操作输出；当且仅当有数据的时候进行Output操作
      if(!rdd.isEmpty()){
        println("============================================")
        // TODO: RDD输出至关重要，往往先降低RDD分区数，在对每个分区数据进行输出操作
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
    })
*/
    /**
      * def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
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


    /**
      * 将结果保存到MySQL数据库中
      */
/*
    wordCountDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        //
        rdd.cache()

        // TODO: 在SparkCore中如何将RDD数据保存MySQL数据库表中编程，再此时一模一样
        rdd.coalesce(1).foreachPartition(iter => {
          // 1. 获取数据库连接Connection

          // 2. 针对分区中的数据插入表中（批量插入）
          iter.foreach(item => {

          })

          // 3. 关闭连接
        })

        //
        rdd.unpersist(true)
      }
    })
*/

    // TODO: 4. 启动实时流式应用，开始准备接收数据并进行处理
    ssc.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
