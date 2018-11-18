package com.erongda.bigdata.spark.risk

import java.util.Properties

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.erongda.bigdata.spark.risk.hbase.HBaseDao
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * 从Kafka Topic中读取数据，采用Direct方式，并且自己管理各个Topic的各个Partition的偏移量
  *
  * TODO: 相关说明如下；
  *     -a. KAFKA 中一个Topic中的数据，存储在一个HBase表中
  *       topic的名称与table的名称是一致的
  *     -b. 实际环境中使用Python依据用户信息（在授权情况下）爬取不同维度的数据，存储在不同的KAFKA TOPIC中
  *        所有TOPIC中的每条数据，都是JSON格式数据，符合标准
  *        {
  *           'id': RowKey,
  *           'f' : info,
  *           'q' : columns,
  *           'v' : values,
  *           't' : timestamp
  *        }
  */
object KafkaDataStreamRedis {

  // Redis中存储Topic消费的偏移量数据
  val REDIS_KEY_CONSUMER_TOPIC_OFFSET = "kafka:topic:offsets"

  // 记录日志信息
  private val logger: Logger = LoggerFactory.getLogger(KafkaDataStreamRedis.getClass)

  /**
    * 程序需要接受三个参数：
    *         第一个参数：每次读取Topic数据，读取几个topic，对应到属性文件
    *         第二个参数：设置批处理时间间隔 batchInterval
    *         第三个参数：设置每个分区每秒中最大条目数 maxRatePrePartition
    * @param args
    */
  def main(args: Array[String]): Unit = {
/*
    if(args.length < 3){
      println("Usage: KafkaDataStreamRedis <kafka_topic_index> <batch_interval> <max_rate>")
      System.exit(-1)
    }
*/
    // 解释参数到变量
    // val Array(kafkaTopicIndex, batchInterval, maxRatePrePartition) = args
    val Array(kafkaTopicIndex, batchInterval, maxRatePrePartition) = Array("1", "3", "2000")

    /**
      * TODO: 1 -> 构建StreamingContext流式上下文对象
      */
    // 1.1 初始化配置，SparkConf实例对象，应该相关配置
    val sparkConf = new SparkConf()
      .setAppName(KafkaDataStreamRedis.getClass.getSimpleName)
      // 设置运行的地方，如果是本地模式需要设置, 集群模式通过spark-submit提交传递
      .setMaster("local[3]")
      // 设置最大条目数
      .set("spark.streaming.kafka.maxRatePerPartition", maxRatePrePartition)
      .set("spark.streaming.backpressure.enabled", "true")
      // 设置序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 1.2 创建SparkContext实例对象
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    // 1.3 创建StreamingContext上下文对象，设置批处理时间间隔
    val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))

    // TODO: i. 加载配置属性文件（关于连接Zookeeper和Kafka Cluster相关配置）
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("kafka.properties"))

    // TODO: ii. 从配置文件中，获取从哪些Topic中读取数据
    val kafkaTopics = props.getProperty(s"kafka.topic.$kafkaTopicIndex")
    if(kafkaTopics == null || kafkaTopics.trim.length <= 0){
      System.err.println("Usage: KafkaDataStreamRedis <kafka_topic_index> is number from kafka.properties")
      System.exit(-1)
    }
    val topics: Set[String] = kafkaTopics.split(",").toSet

    // TODO: iii. 获取消费组GroupId
    val groupName = props.getProperty("group.id")

    // TODO: iv. 构建ZkClient实例对象，用于连接Zookeeper并且操作
    val zkClient: ZkClient = new ZkClient(
      props.getProperty("zk.connect"), // zkServers
      Integer.MAX_VALUE, // sessionTimeout
      1000000, // connectionTimeout
      ZKStringSerializer
    )

    /**
      * TODO: 2 -> 采用Direct方式从Kafka Topics中读取数据
      */
    //  a.
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> props.getProperty("metadata.broker.list"),
      "auto.offset.reset" -> "largest",
      "group.id" -> groupName
    )

    // TODO: 2.1 从 <Redis> 中读取偏移量信息
    //  b. 组装Offset，从Redis中读取
    import scala.collection.mutable
    var fromOffsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()

    // 针对每个Topic获取所有分区Partition的偏移量
    topics.foreach(topicName => {
      // i. 获取Topic有多少个分区，需要到Zookeeper上读取, 通过ZkClient获取某个ZNode孩子数
      // 路径基于Kafka Chroot生成的： /brokers/topics/testtopic0/partitions
      val children: Int = zkClient.countChildren(ZkUtils.getTopicPartitionsPath(topicName))

      // ii. 依据分区，到Redis中获取对应偏移量；如果不存在，偏移量就是0L
      for(partitionId <- 0 until children){
        val field: String = s"${topicName}_$partitionId"
        // TODO: 从Redis读取数据
        Try{
          // 第一步、获取Jedis连接
          val  jedis = JedisPoolUtil.getJedisPoolInstance.getResource

          // 第二步、依据key和field获取offset
          val offset = jedis.hget(REDIS_KEY_CONSUMER_TOPIC_OFFSET, field)

          // 第三步、将该分区和偏移量信息添加到Map中
          if(null != offset){
            fromOffsets += TopicAndPartition(topicName, partitionId) -> offset.toLong
          }else{
            fromOffsets += TopicAndPartition(topicName, partitionId) -> 0L
          }

          // 返回
          jedis
        }match {
          case Success(jedisInstane) => if(null != jedisInstane) JedisPoolUtil.release(jedisInstane)
          case Failure(exception) => throw exception
        }
      }
    })

    // c. 表示从Kafka Topic中获取每条数据以后的处理方式，此处获取topicName和Message数据
     val messageHandler =  (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message())

    // TODO: 2.2 依据偏移量读取数据
    /**
      * R: ClassTag -> (topicName, Message), 不同维度数据存储不通的Topic中，ETL到不同HBase表中（topic -> hbase）
      */
    val messagesDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)]( //
      ssc, //
      kafkaParams, //
      fromOffsets.toMap, //
      messageHandler //
    )

    // 打印数据
    // messagesDStream.print(20)

    /**
      * TODO: 3 -> 将数据ETL到HBase表中
      */
    // TODO: 3.1 将数据保存插入到HBase表中
    messagesDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){

        // 获取当前批次RDD中各个分区数据的偏移量范围
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 对每批次RDD中的数据进行ETL操作，存储HBase表中，在更新各个分区的偏移量
        rdd.foreachPartition(iter => {
          // a. 获取当前分区ID(RDD中每一个分区的数据被一个Task处理，每个Task处理数据的时候有一个上下文对象TaskContext)
          val partitionId: Int = TaskContext.getPartitionId()
          // b. 依据分区ID 获取到 分区数据中对应KAFKA TOPIC分区数据的偏移量信息
          val offsetRange = offsetRanges(partitionId)
          logger.warn(s"${offsetRange.topic} - ${offsetRange.partition}: from[${offsetRange.fromOffset}] to [${offsetRange.untilOffset}]")

          // TODO: ========================================================
/*
          iter.foreach{
            case (topicName, message) => println(topicName + "-->" + message)
          }
          // 将数据插入到HBase表中，HBase表的名称其实就是Topic的名称
*/
          var isInsertSuccess: Boolean = false
          if(offsetRange.topic != null){
            isInsertSuccess = HBaseDao.insert(offsetRange.topic, iter)
          }

          // TODO: 3.2 更新最新偏移量数据到Redis
          if(isInsertSuccess) {
            Try {
              // 获取连接
              val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
              // 更新Redis中偏移量数据
              jedis.hset(
                REDIS_KEY_CONSUMER_TOPIC_OFFSET, //
                offsetRange.topic + "_" + offsetRange.partition, //
                offsetRange.untilOffset.toString //
              )
              // 返回
              jedis
            } match {
              case Success(jedisInstance) => if (null != jedisInstance) JedisPoolUtil.release(jedisInstance)
              case Failure(exception) => exception.printStackTrace()
            }
          }
        })

      }
    })

    /**
      * TODO: 4 -> 启动流式应用
      */
    ssc.start()
    ssc.awaitTermination()

    // 流式应用的停止
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }

}
