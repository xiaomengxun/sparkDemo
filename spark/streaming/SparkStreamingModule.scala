package com.erongda.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * TODO: 针对SparkStreaming实时流式数据处理应用，编写开发模板，将创建StreamingContext和业务分析进行隔离
  */
object SparkStreamingModule {

  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/datas/sparkstreamin/ktrs-ckpt-0000000000"

  /**
    * 贷出模式中贷出函数loan function：获取资源及关闭资源，调用用户函数处理业务数据
    * @param args
    *             程序的参数
    * @param operation
    *                  用户函数，此处为实时接收流式数据并进行处理与输出
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit = {

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
            .setAppName("SparkStreamingModule")
            .set("spark.streaming.kafka.maxRatePerPartition", args(2))
          // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
          val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
          // 设置日志级别
          ssc.sparkContext.setLogLevel("WARN")

          // 设置检查点目录
          ssc.checkpoint(CHECK_POINT_DIRECTORY)
          // TODO：从KAFKA Topic中实时去读数据，进行分析处理，并打印控制台
          operation(ssc)

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
  def processStreamingData(ssc: StreamingContext): Unit ={
    // TODO: .......
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
