package com.erongda.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 从Kafka Topic中读取数据，进行窗口统计（windowInterval = batchInterval * 3）
  *
  * 运行程序：
  *     WindowStreamingWordCount local[3]  5 10000
  */
object WindowStreamingWordCount {

  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/datas/sparkstreamin/ktrs-ckpt-22222222224"

  /**
    * 贷出模式中贷出函数loan function：获取资源及关闭资源，调用用户函数处理业务数据
    * @param args
    *             程序的参数
    * @param operation
    *                  用户函数，此处为实时接收流式数据并进行处理与输出
    */
  def sparkOperation(args: Array[String])(operation: (StreamingContext, Int)=> Unit): Unit = {

    // 判断传递一个参数，设置Spark Application运行的地方
    if(args.length < 3){
      println("Usage: SparkStreamingModule <master> <BatchInterval> <maxRatePerPartition>.......")
      System.exit(1)
    }

    var context: StreamingContext = null
    try{
      // 创建StreamingContext实例对象，考虑高可用性，检查点
      context = StreamingContext.getActiveOrCreate(
        CHECK_POINT_DIRECTORY, //
        () => { //
          // 创建SparkConf实例对象，设置应用相关信息
          val sparkConf = new SparkConf()
            .setMaster(args(0))
            .setAppName("WindowStreamingWordCount")
            .set("spark.streaming.kafka.maxRatePerPartition", args(2))
          // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
          val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
          // 设置日志级别
          ssc.sparkContext.setLogLevel("WARN")

          // 设置检查点目录
          ssc.checkpoint(CHECK_POINT_DIRECTORY)
          // TODO：从KAFKA Topic中实时去读数据，进行分析处理，并打印控制台
          operation(ssc, args(1).toInt)

          // 返回StreamingContext实例对象
          ssc
        }
      )
      // 设置日志级别
      context.sparkContext.setLogLevel("WARN")
      // 启动流式应用
      context.start()
      context.awaitTermination()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      // 关闭StreamingContext
      if(null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * 贷出模式中用户函数（user function)
    * @param ssc
    *            StreamingContext实例对象，读取数据源的数据
    */
  def processStreamingData(ssc: StreamingContext, interval: Int): Unit ={
    // 从kAFkA中读取数据的相关配置信息上的设置
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094",
      "auto.offset.reset" -> "largest"
    )
    // 设置从哪些Topic中读取数据，可以是多个Topic
    val topics: Set[String] = Set("testTopic")
    // 采用Direct方式从Kafka Topic中pull拉取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, //
      kafkaParams, //
      topics
    )

    // 窗口统计, 设置窗口大小
    // val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(interval) * 3)
    val windowDStream: DStream[(String, String)] = kafkaDStream
      .window(Seconds(interval) * 3, Seconds(interval) * 2)
    // 对窗口中的数据进行处理
    val wordCountDStream: DStream[(String, Int)] = windowDStream
      .map(_._2)  // 获取Topic中Message数据
      .transform(rdd => {
        rdd
          // a. 分割单词
          .flatMap(line => line.split("\\s").map(_.trim).filter(_.length > 0))
          // b. 转换为二元组
          .map(word => (word, 1))
          // c. 按照Key聚合统计
          .reduceByKey((a, b) => a + b)
      })
    // 输出
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
  }

  /**
    * Spark Application程序应用的入口，必须创建SparkContext，读取数据和调度Job执行
    * @param args
    *             程序传递的参数
    */
  def main(args: Array[String]): Unit = {
    // 调用贷出函数
    sparkOperation(args)(processStreamingData)
  }

}
