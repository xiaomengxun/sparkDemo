package com.erongda.bigdata.spark.streaming

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时统计最近一段时间（如半个小时或者二十分钟），各个省份的订单量 ，属于窗口统计：
  *     企业中往往 集成SparkSQL进行分析
  *         SparkSQL中数据结构DataFrame或Dataset，更加洞察数据类型，在执行程序之前就可以进行优化
  *     将趋势 统计分析结果存储到MySQL数据库中
  *         dataframe/dataset.write.mode(SaveMode.OVERWRITE).jdbc()
  */
object OrderSQLCountStreaming {

  // batchInterval时间间隔
  val INTERVAL = 5

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("OrderSQLCountStreaming")
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(INTERVAL))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")

    // 设置检查点目录，用于存储Key的状态信息数据
    ssc.checkpoint("/datas/sparkstreaming/ckpt-000000111223")

    /**
      * TODO: 1. 从数据源实时接收流式数据
      */
    // 从kAFkA中读取数据的相关配置信息上的设置
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094",
      "auto.offset.reset" -> "largest"
    )
    // 设置从哪些Topic中读取数据，可以是多个Topic
    val topics: Set[String] = Set("orderTopic")
    // 采用Direct方式从Kafka Topic中pull拉取数据
    val orderDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, //
      kafkaParams, //
      topics
    ).map(_._2)

    // 设置窗口大小、滑动大小（每个任务执行时间间隔）
    val windowDStream = orderDStream.window(Seconds(INTERVAL) * 4, Seconds(INTERVAL) * 2)

    // TODO：2. 集成SparkSQL对窗口中数据进行分析，并将数据保存, 此处使用DStream#foreachRDD输出函数
    // http://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#dataframe-and-sql-operations
    windowDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        // 过滤掉不合格的数据
        val orderRDD = rdd.filter(line => line.trim.length > 0 && line.trim.split(",").length >= 3)

        /**
          * 使用SparkSQL分析，需要将RDD转换为DataFrame：
          *   -1. RDD[CaseClass]
          *     通过CaseClass自动推断Schema信息
          *   -2. 自定义Schema信息
          *     -a. RDD[Row]
          *     -b. val schema: StructType = StructType(Array(StructField))
          *     -c. sparkSession.createDataFrame(rowRDD, schema)
          */
        // TODO: a. 转换RDD数据类型为Row
        val rowRDD: RDD[Row] = orderRDD.map(line => {
          val arr = line.trim.split(",")
          Row(arr(0), arr(1).toInt, arr(2).toDouble)
        })
        // TODO: b. 定义Schema
        val schema: StructType = StructType(
          StructField("order_id", StringType, nullable = true) ::
          StructField("province_id", IntegerType, nullable = true) ::
          StructField("order_price", DoubleType, nullable = true) :: Nil
        )
        // TODO: c. 创建SparkSession实例对象
        val spark: SparkSession = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .config("spark.sql.shuffle.partitions", "3")
          .getOrCreate()
        import spark.implicits._
        // TODO: d. 将RDD转换为DataFrame
        val orderDF: DataFrame = spark.createDataFrame(rowRDD, schema)

        // TODO: e. 使用DSL分析，调用DataFrame API分析
        val orderCountDF = orderDF
          .select($"province_id").groupBy($"province_id").count()

        orderCountDF.show(10, truncate = false)
        // 保存MySQL数据库
        orderCountDF.write.mode(SaveMode.Overwrite)
          .jdbc("jdbc:mysql://bigdata-training01.erongda.com:3306/test?user=root&password=123456", "order_pv", new Properties())
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
