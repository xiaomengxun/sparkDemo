package com.erongda.bigdata.spark.meituan.ad

import java.sql.PreparedStatement
import java.util.Calendar

import com.erongda.bigdata.jedis.JedisPoolUtil
import com.erongda.bigdata.spark.meituan.dao.JDBCHelper
import com.erongda.bigdata.spark.meituan.util._
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * 广告点击流量实时统计分析: 对收集得到的用户点击广告数据进行数据分析，并将结果保存到Redis/MySQL中
    *- 用户点击广告数据存储: Kafka
    *- 数据处理方式: SparkStreaming
    *- 结果保存: Redis/MySQL
  */
object AdClickRealTimeStateSpark {

  // Streaming批次时间间隔
  val BATCH_INTERVAL: Int = 3 // 30
  // 用户黑名单：窗口大小，窗口范围为 10分钟
  val BLACK_LIST_WINDOW_INTERVAL: Int = BATCH_INTERVAL * 2 * 10
  // 用户黑名单：批次/执行间隔大小（滑动窗口大小），批次产生的时间间隔为 3分钟
  val BLACK_LIST_SLIDER_INTERVAL: Int = BATCH_INTERVAL * 2 * 3

  // 实时最近时间窗口统计：窗口大小，窗口范围为 10分钟
  val REAL_TIME_WINDOW_INTERVAL: Int = BATCH_INTERVAL * 2 * 10
  // 实时最近时间窗口统计：批次/执行间隔大小（滑动窗口大小），批次产生的时间间隔为 1分钟
  val REAL_TIME_SLIDER_INTERVAL: Int = BATCH_INTERVAL * 2 * 1

  // 数据分割符
  val delimiter: String = " "
  // 是否是本地执行，初始化设置，具体从配置文件中读取信息
  var isLocal: Boolean = false

  // Redis中黑名单的Key名称
  val REDIS_KEY_USER_BLACK = "ad:user:black"
  val REDIS_KEY_USER_WHITE = "ad:user:white"

  //
  val REDIS_KEY_AD_CLICK_PROVINCE_TOP5 = "ad:click:top5"
  val REDIS_KEY_AD_CLICK_WINDOW = "ad:click:window"

  // Redis中数据存的分隔符
  val REDIS_SEMICOLON_DELIMITER = ":"
  val REDIS_COMMA_DELIMITER = ","

  def main(args: Array[String]): Unit = {

    // TODO：一、 创建上下文
     val (sc, ssc) = {
       // a. 获取相关环境类别
       isLocal = ConfigurationManager.getBoolean(TrackConstants.SPARK_LOCAL)
       val appName = TrackConstants.SPARK_APP_NAME_AD

       // b. 获取SparkConf实例对象，配置应用属性
       val sparkConf = SparkConfUtils.generateSparkConf(appName, isLocal)
       // TODO: 优化设置一：序列化
       sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       sparkConf.registerKryoClasses(Array(classOf[AdClickRecord], classOf[StateDimension]))
       // TODO: 优化设置二：
       // 采用Direct方式从Kafka Topic读取数据，每秒钟每个分区最大条目数
       sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
       // 设置重试次数为2，总共是4次
       sparkConf.set("spark.streaming.kafka.maxRetries", "3")
       // 设置反压机制，自动动态依据Streaming处理数据能力，调整下一次处理数据的条目数
       sparkConf.set("spark.streaming.backpressure.enabled", "true")

       // c. 构建SparkContext实例对象
       val sparkContext = SparkContextUtils.getSparkContext(sparkConf)

       // d. 创建StreamingContext, 设置Batch批次处理时间间隔BatchInterval
       val streamingContext = new StreamingContext(sparkContext, Seconds(BATCH_INTERVAL))

      // e. 由于程序中绘使用updateStateByKey API，需要状态的保存，所以设置checkpoint目录
       val path = "/datas/sparkstreaming/checkpoint/AdClickRealTimeStateSpark"
       // 当Streaming应用第一次运行的时候，先检查目录是否存在，如存在就删除
       FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path(path), true)
       streamingContext.checkpoint(path)

       // f. 返回
       (sparkContext, streamingContext)
     }
    // 关闭日志级别
    sc.setLogLevel("WARN")

    // TODO：二、Kafka集成形成DStream
    val kafkaDStream: InputDStream[(String, String)] = {
      // Kafka configuration parameters
      val kafkaParams: Map[String, String] = Map(
        "metadata.broker.list" -> ConfigurationManager.getProperty(TrackConstants.METADATA_BROKER_LIST),
        "auto.offset.reset" -> ConfigurationManager.getProperty(TrackConstants.AUTO_OFFSET_RESET)
      )
      // Names of the topics to consume
      val  topics: Set[String] = ConfigurationManager
        .getProperty(TrackConstants.TOPIC_NAMES).split(",").toSet[String]
      // 从Kafka Topic中获取数据，返回（key, value)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    // kafkaDStream.print(5)

    // TODO: 三、数据格式转换
    /*
      在真实项目中，这一部分代码可能会比较多，因为Kafka中的数据肯还需要进行必要的格式转换
          (25,1540538414070 province_38 city_138 9196186 2840)
       此处，将Message中Value转换封装到Case Class中即可
     */
    val adClickDStream: DStream[AdClickRecord] = kafkaDStream.transform(rdd => {
      rdd.map(message => {
        // a. 获取Topic中Message的Value值，再按照给定的分隔符进行数据分割（数据必须都不回空）
        val arrs: Array[String] = message._2.split(delimiter).map(_.trim).filter(_.nonEmpty)
        // b. 对符合数据格式要的数据进行数据转换操作
        if(arrs.length == 5){
          // TODO: 在实际开发中，可能在这里一些必要的转换操作，主要依据业务来定
          Some(AdClickRecord(arrs(0).toLong, arrs(1), arrs(2), arrs(3).toInt, arrs(4).toInt))
        }else{
          None // 异常数据，需要丢失，直接返回None对象即可
        }
      }).filter(_.isDefined).map(_.get)
      // TODO: 考虑优化？？ 当RDD的map、filter、flatMap连续存在的时候，最好换成只调用一个API
      /* 思考题：
      rdd.map(msg => msg._2)
        .filter(value => {
          null != value && value.trim.length > 0 && value.trim.split(delimeter).length == 5
        }).flatMap(value => value.split(delimeter))
      */
    })
    // adClickDStream.print(5)

    // TODO：四、黑名单的更新操作
    dynamicUpdateBlackList(adClickDStream)

    // TODO：五、过滤黑名单用户数据
    val filterAdClickDStream: DStream[AdClickRecord] = filterByBlackList(adClickDStream)
    // filterAdClickDStream.transform(rdd => rdd.map(_.userId).distinct()).print(50)

    // TODO：六、实时累加广告点击量
    val aggregateDStream: DStream[(StateDimension, Int)] = calculateRealTimeState(filterAdClickDStream)
    // aggregateDStream.transform(rdd => rdd.sortBy(x => - x._2)).print(20)

    // TODO: 七、获取每日各个省份Top5的累加广告点击量结果
    /*
       输出的表结构：
        * 名称: tb_top5_province_ad_click_count
        * 字段：
          * date 日期
          * province 城市
          * ad_id 广告id
          * click_count 点击次数
        * 插入方式：Insert Or Update
    */
    calculateProvinceTop5Ad(aggregateDStream)

    // 八、分析最近一段时间广告流量点击情况
    /*
      实时统计最近10分钟的某个广告点击数量
        * -1. 窗口大小
          * window interval： 10 * 60 = 600s
        * -2. 执行批次
          * slider interval： 1 * 60 = 60s
      数据结果保存
        * 表名称: tb_ad_click_count_of_window
        * 字段：
          * date: 时间格式字符串
          * ad_id: 广告点击id
          * click_count：点击基础
        * 数据插入方式：Insert Or Error
    */
    calculateAdClickCountByWindow(filterAdClickDStream)

    // 九、启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()

    // 十、关闭Streaming应用
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /**
    * 基于用户点击广告数据进行黑名单更新操作，将黑名单数据写入到Redis中
    * @param adDStream
    *                  广告点击流
    */
  def dynamicUpdateBlackList(adDStream: DStream[AdClickRecord]): Unit ={
    /**
      * a. 规则：
      *   - 最近10分钟用户广告点击次数超过100次（某个用户对所有广告点击次数）
      *   - 黑名单李兵每隔3分钟更新一次
      *   - 如果一个用户被添加到黑名单中，在程序判断中，该用户永远都是黑名单用户；除非工作人员干预，手动删除该用户的黑名单标记
      *   - 支持白名单（白名单中的用户不管点击多少次，都不算是黑名单中存在的）
      * b. 使用DStream的窗口分析函数进行数据更新操作
      *   - 统计用户点击广告次数
      *     次数统计规则：一条数据就是点击一次
      *   - 过滤少于100的数据
      *   - 支持白名单用户过滤
      * c. 白名单用户的数据有工作人员手动添加到Redis中，过滤过程中只需读取即可
      *   优化点：在Streaming应用启动之前从Redis中读取白名单数据，缓存起来，供后期使用
      * d. 无论是黑名单还是白名单
      *     数据量都是很少的，需要实时读取及写入，此处建议使用内存数据库，比如Key/Value类型Redis数据库
      */
    // TODO: 针对DStream开发来说，秉着可以对RDD操作的，就不对DStream操作原则
    adDStream
      // 设置窗口大小： 10分钟； 滑动大小： 3分钟
      .window(Seconds(BLACK_LIST_WINDOW_INTERVAL), Seconds(BLACK_LIST_SLIDER_INTERVAL))
      .foreachRDD(rdd => {
        // ====================================================================
        if(!rdd.isEmpty()){
          // RDD 数据缓存
          rdd.persist(StorageLevel.MEMORY_AND_DISK)

          // 构建SparkSession实例对象
          val spark: SparkSession = SparkSession.builder()
            .config(rdd.sparkContext.getConf)
            .config("spark.sql.shuffle.parititons", "8")
            .getOrCreate()
          import spark.implicits._

          // 将白名单数据广播出去
          val broadcastOfWhiteList: Broadcast[List[Int]] = UserWhiteListSingleton.getInstance(rdd.sparkContext)

          // 将窗口内数据RDD转换为DataFrame，按照各个用户ID进行分组，统计每个用户点击广告次数
          rdd //
            .toDS() // 由于窗口较大（10分钟），数量较大，此处集成SparkSQL分析（采用DSL）
            .select($"userId", $"adId").groupBy($"userId").count() // 分组统计
            .filter($"count" > 100).rdd.coalesce(1) // 转换为RDD并降低分区数
            // 采用调用RDD中foreachPartition API 进行输出操作
            .foreachPartition((iter: Iterator[Row]) => {
              Try{
                // 获取 Jedis连接
                val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
                // 先过滤白名单用户，再插入Redis中
                iter.foreach{ case Row(userId: Int, _: Long) =>
                  // 使用白名单过滤
                  if(!broadcastOfWhiteList.value.contains(userId)){
                    // 将黑名单插入Redis的set无序集合中
                    jedis.sadd(REDIS_KEY_USER_BLACK, userId.toString)
                  }
                }
                // 返回
                jedis
              }match {
                case Success(jedisValue) => JedisPoolUtil.release(jedisValue)
                case Failure(exception) => throw exception
              }
            })

          // 释放缓存
          rdd.unpersist(true)
        }
        // ====================================================================
      })
  }

  /**
    * 根据Redis中黑名单进行数据过滤操作
    * @param adDStream
    *                  广告点击流数据
    * @return
    */
  def filterByBlackList(adDStream: DStream[AdClickRecord]): DStream[AdClickRecord] = {
    // 将DStream数据的过滤操作转换为RDD数据的过滤
    adDStream.transform(rdd => {
      // i. 从Redis中读取黑名单数据
      val blackListUsers: List[String] = {
        Try{
          // 获取 Jedis连接
          val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
          // 从Redis中依据Key读取黑名单用户
          import scala.collection.JavaConverters._
          val blackSetUsers: mutable.Set[String] = jedis.smembers(REDIS_KEY_USER_BLACK).asScala
          // 返回
          (jedis, blackSetUsers.toList)
        }match {
          case Success((jedisValue, blackUsers)) =>
            // 关闭连接
            JedisPoolUtil.release(jedisValue)
            // 返回
            blackUsers
          case Failure(exception) => throw exception
        }
      }

      // ii. 过滤黑名单中的数据（过滤分为Map端过滤），基于广播变量实现
      val broadcastOfBlackList: Broadcast[List[Int]] = rdd.sparkContext
          .broadcast(blackListUsers.map(_.toInt))

      // iii. 对RDD调用mapPartitions函数迭代遍历过滤数据
      rdd.mapPartitions(iter => {
        iter.filter(adClickRecord => !broadcastOfBlackList.value.contains(adClickRecord.userId))
      })
      // ----------------------------------------------------------
    })
  }

  /**
    * 实时累加统计各个广告点击流量：
        a. 维度信息：每天 每个省份 每个城市
        b. DStream[((date, 省份, 城市， 广告), 点击量)
    *
    * @param adDStream
    *                  过滤后的广告点击流量数据
    */
  def calculateRealTimeState(adDStream: DStream[AdClickRecord]): DStream[(StateDimension, Int)] = {
    // 1. 将adDStream转换为Key/Value对形式   DStream[(StateDimension, Int)]
    val mappedDStream = adDStream.transform(rdd => {
      rdd //
        .map{ case AdClickRecord(timestamp, province, city, userId, adId) =>
          // 1.a. 根据timestamp获取时间格式字符串，格式为:yyyyMMdd
          val date = DateUtils.parseLong2String(timestamp, "yyyyMMdd")
          // 1.b. 返回结果
          (StateDimension(date, province, city, adId), 1)
        }
        .reduceByKey(_ + _) // 累加更新状态之前，先进行 聚合操作
    })

    // 2. 累加计算结果值  => 考虑使用updateStateByKey API实现
    /**
      * 方式一：updateStateByKey：
      *     随着数据规模的执行时间延长，结果数据会越来越长，对性能会有一定影响
      *       - 某些不会出现的Key， updateSateByKey会进行保存
      *       - 可以通过返回None的形式，表示不缓存该Key的数据
      * 方式二：mapWithState (推荐使用）
      *     可以缓解updateStateByKey API的问题
      */
    // TODO: 建议如果进行实时累加统计分析，先聚合再更新状态，较少数据量，优化性能
    val aggregateDStream = mappedDStream
      // 更新函数要求：updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(
      /**
        * TODO：状态是二元组(counter, stateValue)
        *   -a. 第一个元素Key表示的是记录当前Key连续多少次没有值传递进来
        *   -b. 第二各元素Value表示的是当前Key的状态的值
        */
        (values: Seq[Int], state: Option[(Long, Int)]) => {
          // i. 获取当前批次中状态的值
          val currentValue = values.sum
          // ii. 获取以前的状态的值
          val counters: Long = state.getOrElse((0L, 0))._1
          val previousValue: Int = state.getOrElse((0L, 0))._2

          // iii. 更新状态值，考虑判断计数器
          if(currentValue == 0){ // 当前批次Key没有数据传递进来
            // 连续10个批次，当前Key的都没有数据传递进来
            if(counters > 24 * 60 * 2){
              // TODO: 计数器counters非常重要，要依据实际需求合理设置，要保证数据过期对累加计算没有影响
              None
            }else{
              Some(counters + 1L, previousValue)
            }
          }else{ // 有新数据，合并和更新
            Some(0L, currentValue + previousValue)
          }
        }
      )
      // 提取值
      .map{
        case (stateDimmension, (_, clickCount)) => (stateDimmension, clickCount)
      }

    // 3. 需要将数据输出到关系型数据库MySQL表中
    /**
      * 表的结构：
      *   名称： tb_ad_real_time_state
      *   字段：
      *     date 日期
      *     province 省份
      *     city 城市
      *     ad_id 广告ID
      *     click_count 点击次数
      *   其中 (date, province, city, ad_id) 为主键 -> 联合主键
      *     数据插入方式Insert Or Update
      */
    aggregateDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        // =====================================================
        rdd.coalesce(2).foreachPartition(iter => {
          Try{
            // 1.获取链接
            val conn = JDBCHelper.getMySQLConnection

            // 数据库默认事务设置
            val oldAutoCommit = conn.getAutoCommit
            conn.setAutoCommit(false)

            // 2. 创建Statement对象
            val sqlStr = "INSERT INTO tb_ad_real_time_state(`date`, `province`, `city`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `click_count`=VALUES(`click_count`)"
            val pstmt: PreparedStatement = conn.prepareStatement(sqlStr)

            // 3. 对数据进行迭代输出操作
            var recordCounter = 0
            iter.foreach{ case (StateDimension(date, province, city, adId), clickCount) =>
              // 设置参数
              pstmt.setString(1, date)
              pstmt.setString(2, province)
              pstmt.setString(3, city)
              pstmt.setInt(4, adId)
              pstmt.setInt(5, clickCount)

              // 添加批次
              pstmt.addBatch()
              recordCounter += 1

              // 提交
              if(recordCounter % 500 == 0) {
                pstmt.executeBatch()
                conn.commit()
              }
            }
            // 4. 提交
            pstmt.executeBatch()
            conn.commit()

            // 5. 返回结束
            (oldAutoCommit, conn)
          }match {
            case Success((oldAutoCommitValue, connMySQL)) =>
              Try(connMySQL.setAutoCommit(oldAutoCommitValue))
              if(null != connMySQL) connMySQL.close()
            case Failure(exception) => throw exception
          }
        })
        // =====================================================
      }
    })

    //  4. 返回
    aggregateDStream
  }

  /**
    * 计算每日各个省份Top5热门广告数据
    *   考虑字段：
    *     日期，省份、广ID -> 点击量
    * @param aggDStream
    *                   实时累加统计的广告点击量（维度信息：日期、省份、城市、广告ID）
    */
  def calculateProvinceTop5Ad(aggDStream: DStream[(StateDimension, Int)]): Unit ={
    // TODO: 针对RDD进行分组操作，获取各省份每个广告的点击流量 ,使用aggregateByKey聚合操作
    // DStream[((String, String), ListBuffer[(Int, Int)])]
    val top5ProvinceAdClickCountDStream = aggDStream.transform(rdd => {
      // TODO: 1. 实时对每日各省份广告点击流量聚合统计
      val provinceAdClickCountRDD: RDD[((String, String), (Int, Int))] = rdd
        // i,针对RDD的分区进行数据转换，提取需要的字段信息
        .mapPartitions(iter => {
          iter.map{ case(stateDim, clickCnt) =>
            ((stateDim.date, stateDim.province, stateDim.adId), clickCnt)
          }
        })
        // ii. 聚合统计：计算各个省份广告点击量
        .reduceByKey(_ + _)
        // iii. 数据转换
        .mapPartitions(iter => {
          iter.map{ case((date, province, adId), clickCnt) =>
            ((date, province), (adId, clickCnt))
          }
        })

      // TODO：2. 采用aggregateByKey实现各省份Top5点击广告
      /**
        * // TODO: 对于RDD聚合来说，由于分布式数据集（由多个分区Partition组成），所以先对分区中数据进行局部聚合，再对各个分区聚合的结果进行全局聚合。
        *  def aggregateByKey[U: ClassTag] -> 针对相同Key的Values进行聚合操作编码
        *  // 聚合时各个分区聚合中聚合中间临时变量的值初始化
        *  (zeroValue: U)
        *  (
        *    seqOp: (U, V) => U,  // 各个分区如何聚合
             combOp: (U, U) => U // 各分区聚合结果的聚合
           ): RDD[(K, U)]
        */
      provinceAdClickCountRDD.aggregateByKey(mutable.ListBuffer[(Int, Int)]())(
        (u, v) => {  // 分区内聚合：seqOp: (U, V) => U
          // 将每个元素加入到ListBuffer中
          u += v
          // 对ListBuffer中数据进行降序排序，获取Top5
          u.sortBy(- _._2).take(5)
        },
        (u1, u2) => { // 分区间聚合：combOp: (U, U) => U
          // 合并两个分区的聚合结果ListBuffer
          u1 ++= u2
          // 对ListBuffer中数据进行降序排序，获取Top5
          u1.sortBy(- _._2).take(5)
        }
      )
    })

    // TODO: 结果输出
    // top5ProvinceAdClickCountDStream.print(40)
    /**
      * 将结果保存到Redis数据库中
      *   - Key 名称:
      *     ad:click:top5
      *   - Value 类型：
      *     哈希结构hash
      *         field -> date_provinceId
      *         value -> adId:clickCount,adId:clickCount,adId:clickCount
      */
    top5ProvinceAdClickCountDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        // RDD 缓存
        rdd.persist(StorageLevel.MEMORY_AND_DISK)

        // =====================================================
        rdd.coalesce(2).foreachPartition(iter => {
          Try{
            // 获取 Jedis连接
            val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
            // 对数据进行迭代操作
            iter.foreach{ case ((date, province), list) =>
              // 组合 省份名称和日期 为 field
              val field = date + REDIS_SEMICOLON_DELIMITER + province
              // 针对各省份Top5的广告ID和点击次数组合
              val topAdClickList = new mutable.ListBuffer[String]()
              for((adId, clickCount) <- list.toList){
                topAdClickList += adId + REDIS_SEMICOLON_DELIMITER + clickCount
              }
              // 存入Redis中，数据类型为哈希hash
              jedis.hset(
                REDIS_KEY_AD_CLICK_PROVINCE_TOP5, //
                field, topAdClickList.mkString(REDIS_COMMA_DELIMITER) //
              )
            }
            // 返回
            jedis
          }match {
            case Success(jedisValue) => JedisPoolUtil.release(jedisValue)
            case Failure(exception) => throw exception
          }
        })
        // =====================================================

        // 释放内存
        rdd.unpersist(true)
      }

    })
  }


  /**
    * 实时统计最10分钟的某广告点击流量
    *     使用Window进行分析的一个窗口分析函数（窗口分析函数）
    * @param adDStream
    *                  过滤后的广告点击流数据
    */
  def calculateAdClickCountByWindow(adDStream: DStream[AdClickRecord]): Unit ={
    // a. 针对RDD的各个分区数据进行转换操作
    val adTupleDStream: DStream[(Int, Int)] = adDStream
      .transform(rdd => {
        // def mapPartitions(f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
        rdd.mapPartitions(
          _.map(record => (record.adId, 1)), preservesPartitioning = true
        )
      })

    // b. 窗口聚合统计
    val adWindowDStream: DStream[(Int, Int)] = adTupleDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 +v2 , // reduceFunc: (V, V) => V
      (v1: Int, v2: Int) => v1 - v2, // invReduceFunc: (V, V) => V
      Seconds(REAL_TIME_WINDOW_INTERVAL), //
      Seconds(REAL_TIME_SLIDER_INTERVAL)
    )

    // c. 输出 窗口统计结果
    adWindowDStream.foreachRDD((rdd, time) => {
      if(!rdd.isEmpty()){
        // 获取窗口开始时间
        val startWindowTime: String = DateUtils.parseLong2String(
          time.milliseconds, "yyyy/MM/dd HH:mm:ss", Calendar.SECOND, - REAL_TIME_WINDOW_INTERVAL
        )
        // 获取窗口结束时间
        val endWindowTime: String = DateUtils.parseLong2String(
          time.milliseconds, "yyyy/MM/dd HH:mm:ss")
        // println(s"startWindowTime = $startWindowTime - endWindowTime = $endWindowTime")

        // 将结果数据保存Redis数据库，插入之前清空表中所有数据
        // =====================================================
        rdd.coalesce(2).foreachPartition(iter => {
          Try{
            // 获取 Jedis连接
            val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
            // TODO：先删除存储窗口统计数据，再插入
            jedis.del(REDIS_KEY_AD_CLICK_WINDOW)
            // 对数据进行迭代操作
            iter.foreach{ case (adId, clickCount) =>
              // 存入Redis中，数据类型为哈希hash
              jedis.hset(
                REDIS_KEY_AD_CLICK_WINDOW, //
                startWindowTime + REDIS_SEMICOLON_DELIMITER + endWindowTime + REDIS_COMMA_DELIMITER + adId, //
                clickCount.toString //
              )
            }
            // 返回
            jedis
          }match {
            case Success(jedisValue) => JedisPoolUtil.release(jedisValue)
            case Failure(exception) => throw exception
          }
        })
        // =====================================================

      }
    })

  }

}





