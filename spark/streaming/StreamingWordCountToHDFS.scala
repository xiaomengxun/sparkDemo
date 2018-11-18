package com.erongda.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Socket中读物数据，分析处理，将结果打印到 控制台上， 实时接收数据，统计每批次数据中词频WordCount
  */
object StreamingWordCountToHDFS {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("StreamingWordCount")
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
    wordCountDStream.print(10)

    // 将每批次处理结果数据保存至HDFS目录中
    // 此种方式不推荐使用
    wordCountDStream.saveAsTextFiles(prefix = "order", suffix = "logs")

    // TODO: 推荐使用如下方式：
    wordCountDStream.foreachRDD((rdd, time) => {
      // 降低分区数据：1) 保存文件数表少 2) HDFS 擅长存储大数据文件
      rdd.coalesce(1).saveAsTextFile("......")
    })


    // TODO: 4. 启动实时流式应用，开始准备接收数据并进行处理
    ssc.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
