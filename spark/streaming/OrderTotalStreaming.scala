package com.erongda.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 从Kafka Topic中读取数据，分析处理，将结果打印到 控制台上：
  *     实时累加统计 各省份销售订单的总销售额
  */
object OrderTotalStreaming {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("OrderTotalStreaming")
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")

    // 设置检查点目录，用于存储Key的状态信息数据
    ssc.checkpoint("/datas/sparkstreaming/ckpt-000000")

    /**
      * TODO: 1. 从数据源实时接收流式数据
        def createDirectStream[
          K: ClassTag,
          V: ClassTag,
          KD <: Decoder[K]: ClassTag,
          VD <: Decoder[V]: ClassTag] (
            ssc: StreamingContext,
            kafkaParams: Map[String, String],
            topics: Set[String]
        ): InputDStream[(K, V)]
      */
      // 从kAFkA中读取数据的相关配置信息上的设置
      val kafkaParams: Map[String, String] = Map(
        "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094",
        "auto.offset.reset" -> "largest"
      )
      // 设置从哪些Topic中读取数据，可以是多个Topic
      val topics: Set[String] = Set("orderTopic")
      // 采用Direct方式从Kafka Topic中pull拉取数据
      val kafkaDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, //
        kafkaParams, //
        topics
      ).map(_._2)

    // TODO：2. 调用DStream中API对数据分析处理
    /**
      * Kafka Topic中每条Message数据格式：
      *     字段信息：orderId,provinceId,orderPrice
      */
    val orderDStream: DStream[(String, Double)] = kafkaDStream.transform(rdd => {
      rdd
        // 表示过滤不合格的数据
        .filter(line => line.trim.length > 0 && line.trim.split(",").length >= 3)
        // 提取字段信息：provinceId,orderPrice
        .map(line => (line.split(",")(1), line.split(",")(2).toDouble))
    })


    // TODO: 实时累加统计各个订单的销售额，按照各个省份进行累加统计
    /**
      * 字面意思:
      *     依据Key(provinceId) 来更新 状态State信息（TotalPrice）的
      *   def updateStateByKey[S: ClassTag](
            updateFunc: (Seq[V], Option[S]) => Option[S]
          ): DStream[(K, S)]

          参数含义：updateFunc ->  更新状态函数
              对于当前应用来说：
                    V ->  每个订单的销售额，Double    S ->  对应省份的总的销售额，类型是Double
              第一个参数：Seq[V]
                  当前批次Batch处理数据中，某个Key的所有Value的集合
              第二个参数：Option[S]
                  表示的是前一批次中某个Key的状况信息，用于合并当前批次中Key的状态信息的
          回顾；MapReduce中reduce聚合函数
            public void reduce(Text key, Iterable[IntWritable] values) { // ..... }

          // TODO: Streaming 来说，针对每批次RDD数据进行处理的
          1 -> 第一批次执行
              bj -> 100
              sh -> 300
              hz -> 240
          2 -> 第二批次执行
              bj -> 1000  + 100  =  1100
              sh -> 40    + 300  =  340
              nj -> 80    + 0    =  80
          // TODO: 实时累加统计思想：
                当前批次统计状态（Key） + 前一批次的统计状态（Key） = 所有统计状态
      */
    val priceDStream: DStream[(String, Double)] = orderDStream.updateStateByKey(
      (values: Seq[Double], state: Option[Double]) => {
        // 1. 获取当前Key的以前状态信息
        val previousState = state.getOrElse(0.0)
        // 2. 计算当前批次中Key的状态数据
        val currentState = values.sum
        // 3. 合并状态并返回
        Some(previousState + currentState)
      }
    )


    /**
      * TODO：3. 将每批次处理的结果进行输出
      *     每批次的结果输出
      *       def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
      */
    priceDStream.foreachRDD((rdd: RDD[(String, Double)], time: Time) => {
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
