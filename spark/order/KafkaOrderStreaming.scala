package com.erongda.bigdata.spark.order

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * SparkStreaming实时从Kafka Topic中读取数据（仿双十一订单数据分析）：
  *     -a. 实时累加统计 各省份销售订单额
  *         updateStateByKey   -> Redis中（使用哈希数据类型）
  *     -b. 统计最近20秒各个省份订单量 -> BatchInterval: 4 秒， slide：8秒
  *         SparkSQL（SQL）分析 -> 数据库中
  *  联动测试，每批次处理数据量最多为
  *       8000 * 3 * 4 = 96000 条
  */
object KafkaOrderStreaming {
  // 设置Streaming Application检查点目录
  val CHECK_POINT_DIRECTORY = "/data/spark/streaming/ckpt11-000000"

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
      println("Usage: KafkaOrderStreaming <master> <batchInterval> .............")
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
          .setAppName("KafkaOrderStreaming")
          .setMaster(args(0))
          // 设置Kryo序列化
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .registerKryoClasses(Array(classOf[Order]))
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
      "metadata.broker.list" -> ConstantUtils.METADATA_BROKER_LIST,
      "auto.offset.reset" -> ConstantUtils.AUTO_OFFSET_RESET
    )
    // Kafka中Topic的名称
    val topics: Set[String] = Set(ConstantUtils.ORDER_TOPIC)
    // 从Kafka Topic获取数据，返回(key, message)
    val kafkaDStream: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext
      kafkaParams, // Map[String, String]
      topics // Set[String]
    )
    // kafkaDStream.print(5)

    // TODO 2. 解析Message（JSON格式字符串数据）数据为Order对象
    val orderDStream: DStream[(Int, Order)] = kafkaDStream.transform(rdd => {
      rdd.map(msg => {
        val mapper = ObjectMapperSingleton.getInstance()
        // 解析JSON
        val order = mapper.readValue(msg._2, classOf[Order])
        // 以二元组形式返回
        (order.provinceId, order)
      })
    })
    // orderDStream.print(5)

    // TODO: 将解析的订单数据存储到Kafka Topic中
    // saveDStreamToKafka(orderDStream)

    // TODO: 实时累加统计 各省份销售订单额
    // realTimeOrderTotal(orderDStream)
    realTimeOrderTotalState(orderDStream)

    // TODO: 窗口统计 各个省份销售订单量
    // windowOrderCount(orderDStream)
  }

  /**
    * 统计最近20秒各个省份订单量 -> BatchInterval: 4 秒， slide：8秒
    *         SparkSQL（SQL）分析 -> 数据库中
    * @param orderDStream
    *                     DStream流
    */
  def windowOrderCount(orderDStream: DStream[(Int, Order)]): Unit ={
    // TODO: SparkStreaming窗口统计集成SparkSQL完成
    orderDStream.window(Seconds(20), Seconds(8)).foreachRDD((rdd, time) => {
      val slideTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
      println(s"==================== $slideTime ======================")
      if(!rdd.isEmpty()){
        // RDD 缓存
        rdd.persist(StorageLevel.MEMORY_AND_DISK)

        // 构建SparkSession实例对象
        val spark: SparkSession = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .config("spark.sql.shuffle.partitions", "6")
          .getOrCreate()
        // 隐式导入
        import spark.implicits._

        // 将RDD转换为DataFrame，此处Order为CaseClass，可通过反射方式自动推断Schema信息
        val orderDS: Dataset[Order] = rdd.map(_._2).toDS()

        // 基于DSL分析，首先注册为临时视图
        orderDS.createOrReplaceTempView("view_tmp_order")
        // 编写SQL语句
        val top5ProvinceOrderCountDF: DataFrame = spark.sql(
          """
            |SELECT provinceId, COUNT(1) AS cnt FROM view_tmp_order GROUP BY provinceId ORDER BY cnt DESC LIMIT 5
          """.stripMargin)
        //
        top5ProvinceOrderCountDF.show(5, truncate = false)
        // 保存MySQL数据库表中
        top5ProvinceOrderCountDF.write.mode(SaveMode.Overwrite)
          .jdbc("jdbc:mysql://bigdata-training01.erongda.com:3306/test?user=root&password=123456", "order_cnt", new Properties())
        rdd.unpersist(true)
      }
    })
  }

  /**
    * 实时累加统计 各省份销售订单额
    *         updateStateByKey   -> Redis中（使用哈希数据类型）
    * @param orderDStream
    *                     DStream流
    */
  def realTimeOrderTotalState(orderDStream: DStream[(Int, Order)]): Unit ={
    // 实时累加统计省份销售订单额，更新状态函数 -> mapWithState
    /**
      *   def mapWithState[StateType: ClassTag, MappedType: ClassTag](
            spec: StateSpec[K, V, StateType, MappedType]
          ): MapWithStateDStream[K, V, StateType, MappedType]
      */
    val orderTotalDStream: DStream[(Int, Double)] = orderDStream
      // TODO：在更新状态之前，先对数据进行聚合操作
      .transform(rdd => {
        rdd.reduceByKey((o1, o2) => {
          val batchOrderPrice = o1.orderPrice + o2.orderPrice
          Order(o1.orderId, o1.provinceId, batchOrderPrice)
        })
      })
      .mapWithState[Double, (Int, Double)](
        StateSpec.function(
          // mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
          (provinceId: Int, orderOption: Option[Order], state: State[Double]) => {
            // i. 获取当前Key的状态信息
            val currentPrice: Double = orderOption match {
              case Some(order) => order.orderPrice
              case None => 0.0
            }
            // ii. 获取当前Key以前的状态
            val previousPrice = if(state.exists()) state.get() else 0.0
            // iii. 合并状态
            val lastestPrice = currentPrice + previousPrice
            // iv. 更新状态
            state.update(lastestPrice)
            // v. 返回
            (provinceId, lastestPrice)
          }
        )
    )

    // 将统计各省份销售订单额存储Redis中
    orderTotalDStream.foreachRDD((rdd, time) => {
      println(s"---------------------- $time ---------------------------")
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
                println(provinceId + "->" + orderTotal)
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
    // ========================================================================
  }


  /**
    * 实时累加统计 各省份销售订单额
    *         updateStateByKey   -> Redis中（使用哈希数据类型）
    * @param orderDStream
    *                     DStream流
    */
  def realTimeOrderTotal(orderDStream: DStream[(Int, Order)]): Unit ={
    // 实时累加统计省份销售订单额，更新状态函数 -> updateFunc: (Seq[V], Option[S]) => Option[S]
    val orderTotalDStream: DStream[(Int, Double)] = orderDStream.updateStateByKey(
      (orders: Seq[Order], state: Option[Double]) => {
        // 计算当前批次中省份的总的订单销售额
        val currentTotal: Double = orders.map(_.orderPrice).sum
        state match {
          case Some(previousTotal) => Some(previousTotal + currentTotal)
          case None => Some(currentTotal)
        }
      }
    )

    // 将统计各省份销售订单额存储Redis中
    orderTotalDStream.foreachRDD((rdd, time) => {
      println(s"---------------------- $time ---------------------------")
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
                println(provinceId + "->" + orderTotal)
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
    // ========================================================================
  }

  /**
    * 将DStream保存至KafkaTopic中
    * @param dstream
    *                DStream流
    */
  def saveDStreamToKafka(dstream: DStream[(Int, Order)]): Unit ={
    /**
      * 创建Topic命令：
      *     bin/kafka-topics.sh --create --zookeeper bigdata-training01.erongda.com:2181/kafka --replication-factor 2 --partitions 3 --topic order
      *
      * Console 消费Topic中数据：
      *     bin/kafka-console-consumer.sh --zookeeper bigdata-training01.erongda.com:2181/kafka --topic order
      */
    dstream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        // 设置分区数，最好与Topic分区倍数的关系
        rdd.coalesce(3).foreachPartition(iter => {
          // i. 获取Producer实例对象
          var producer: KafkaProducer[String, String] = null
          try{
            val props: Properties = new Properties()
            props.put(ConstantUtils.BOOTSTRAP_SERVERS_NAME, ConstantUtils.METADATA_BROKER_LIST)
            props.put(ConstantUtils.KEY_SERIALIZER_NAME, ConstantUtils.NEW_SERIALIZER_CLASS)
            props.put(ConstantUtils.VALUE_SERIALIZER_NAME, ConstantUtils.NEW_SERIALIZER_CLASS)
            // 生产者Producer实例对象
            producer = new KafkaProducer[String, String](props)
            // 插入数据到Kafka Topic中
            iter.foreach{
              case (provinceId, order) =>
                val record = new ProducerRecord[String, String](
                  ConstantUtils.SEND_KAFKA_TOPIC_NAME, order.orderId.toString, order.toString
                )
                // 发送数据
                producer.send(record)
            }
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            // iii. 关闭连接
            if(null != producer) producer.close()
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



object ObjectMapperSingleton {
  // 使用transient注解标识的时候，表示此变量不会变序列化
  @transient  private var instance: ObjectMapper = _

  def getInstance(): ObjectMapper = synchronized{
    if (instance == null) {
      instance = new ObjectMapper()
      instance.registerModule(DefaultScalaModule)
    }
    instance
  }
}
