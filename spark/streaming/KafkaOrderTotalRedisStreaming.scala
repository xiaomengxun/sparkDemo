package com.erongda.bigdata.spark.streaming

import com.erongda.bigdata.jedis.JedisPoolUtil
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * SparkStreaming实时从Kafka Topic中读取数据，进行实时统计各省份销售订单额，并将统计结果保存到Redis内存数据库中
  *     Kafka（Zookeeper） -> SparkStreaming(HDFS、YARN、Zookeeper） -> Redis
  *
  *  TODO：
  *     Spark Streaming 反压（Back Pressure）机制
  *     参考文档：
  *       https://www.iteblog.com/archives/2323.html?from=related
  *       https://blog.csdn.net/u010454030/article/details/54629049
  *       https://blog.csdn.net/xiao_jun_0820/article/details/50715623
  */
object KafkaOrderTotalRedisStreaming {

  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/data/spark/streaming/ckptkotf-00000000000000005"

  // 存储Redis的Key
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "order:price:total"

  /**
    * 用于创建StreamingContext实例对象，读取流式数据，实时处理与输出
    * @param args
    *             程序的参数
    * @param operation
    *                  读取流式数据、实时处理与结果输出地方
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit ={

    // 判断传递的参数，设置Spark Application运行的地方
    if(args.length < 2){
      println("Usage: KafkaOrderTotalRedisStreaming <master> <batchInterval> .............")
      System.exit(0)
    }

    var context: StreamingContext = null
    try{
      // 创建StreamingContext实例对象
      context = StreamingContext.getActiveOrCreate(
        CHECK_POINT_DIRECTORY, // 非第一次运行流式应用，重启从检查点目录构建StreamingContext
        () => {  // 第一次运行流式应用 ，创建StreamingContext
          // i. Spark Application配置
          val sparkConf = new SparkConf()
            .setAppName("KafkaOrderTotalRedisStreaming")
            .setMaster(args(0))
          // 设置批处理时间间隔batchInterval
          val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
          // 设置检查点目录
          ssc.checkpoint(CHECK_POINT_DIRECTORY)

          // 真正处理流式数据
          operation(ssc)

          // 返回StreamingContext实例对象
          ssc
        }
      )
      context.sparkContext.setLogLevel("WARN")
      // 启动Streaming应用
      context.start()
      context.awaitTermination()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * SparkStreaming从Kafka接收数据，依据业务实时处理分析，将结果输出到Redis内存数据库
    * @param ssc
    *            SteamingContext实例对象
    */
  def processStreamingData(ssc: StreamingContext): Unit ={
    // TODO 1. 从Kafka中读物数据，采用Direct方式
    // Kafka Cluster连接配置相关参数
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094",
      "auto.offset.reset" -> "largest"
    )
    // Kafka中Topic的名称
    val topics: Set[String] = Set("orderTopic")
    // 从Kafka Topic获取数据，返回(key, message)
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext
      kafkaParams, // Map[String, String]
      topics // Set[String]
    )
    // kafkaDStream.print(5)

    // TODO 2.实时累加统计销售订单额，调用DStream#updateStateByKey
    val orderTotalDStream: DStream[(Int, Double)] = kafkaDStream
      // 转换读取到Message，返回二元组（provinceId, orderPrice)
      .transform(rdd => {
        rdd.map(_._2)
          .filter(line => line.trim.length > 0 && line.trim.split(",").length >= 3)
          .map(line => (line.split(",")(1).toInt, line.split(",")(2).toDouble))
      })
      // 实时累加更新函数： updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(
        (values: Seq[Double], state: Option[Double]) => {
          // 获取当前批次中Key的状态（总的订单销售额）
          val currentTotal = values.sum
          state match {
            case Some(previousTotal) => Some(previousTotal + currentTotal)
            case None => Some(currentTotal)
          }
        }
      )

    // TODO 3. 将实时累加销售订单额存储到Redis内存数据库中
    // orderTotalDStream.print()
    orderTotalDStream.foreachRDD(rdd => {
      // 判断当前批次输出RDD是否有数据
      if(!rdd.isEmpty()){
        // 将RDD保存Redis数据库中，降低分区数, 对RDD的每个分区进行操作
        rdd.coalesce(1).foreachPartition(iter => {
          // i. 获取 Redis数据库连接，通过连接池获取
          var jedis: Jedis = null
          try{
            jedis = JedisPoolUtil.getJedisPoolInstance.getResource
            // ii. 插入数据，采用数据结构为哈希hash
            iter.foreach{
              case (provinceId, orderTotal) =>
                jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, orderTotal.toString)
            }
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            // iii. 关闭连接
            if(null != jedis) JedisPoolUtil.release(jedis)
          }
        })
      }
    })
  }

  /**
    * Spark Application程序入口，必须创建SparkContext（SparkSession或StreamingContext）
    *     程序的运行：
    *         KafkaOrderTotalRedisStreaming <master> <batchInterval>
    * @param args
    *             程序传递参数
    */
  def main(args: Array[String]): Unit = {
    sparkOperation(args)(processStreamingData)
  }

}
