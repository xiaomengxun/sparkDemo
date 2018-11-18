package com.erongda.bigdata.spark.etl

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Partitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Kian on 2018/10/30.
  */
object ELTKafka2HBase {

  // HBase 表中的列簇
  val HBASE_ETL_TABLE_FAMILY_BYTES = Bytes.toBytes("info")
  // HBase 表中的列名
  val HBASE_ETL_TABLE_COLUMN_BYTES = Bytes.toBytes("value")

  def main(args: Array[String]): Unit = {

    // 创建SparkConf实例对象，设置应用相关信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("ELTKafka2HBase")
    // 构建StreamingContext流式上下文实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")

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
    val topics: Set[String] = Set("orders")
    // 采用Direct方式从Kafka Topic中pull拉取数据
    val kafkaDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, //
      kafkaParams, //
      topics
    ).map(_._2)


    kafkaDStream
      .foreachRDD(rdd => {
        // 1. 解析JSON格式字符串，获取 orderType类型
        val orderRDD = rdd.mapPartitions(iter => {
          iter.map(item => {
            // 解析获取orderType
            val orderType = JSON.parseObject(item).getString("orderType")
            // 返回
            (orderType, item)
          })
        })

        // 2. 分区，自定义分区器
        val partitionRDD = orderRDD.partitionBy(
          // 此处定义的是匿名的内部类
          new Partitioner{
            override def numPartitions: Int = 4
            override def getPartition(key: Any): Int = {
              // "alipay", "weixin", "card", "other"
              key.asInstanceOf[String] match {
                case "alipay" => 0
                case "weixin" => 1
                case "card" => 2
                case _ => 3
              }
            }
          }
        )

        // 3. 将分区数据写入HBase表
        /**
          *  "alipay", "weixin", "card", "other"
          *   info: value
          */
        partitionRDD.foreachPartition(iter => {
          // 通过TaskContext获取当前处理分区数据的分区ID
          TaskContext.getPartitionId() match {
            case 0 => insertIntoHBase("htb_alipay", iter)
            case 1 => insertIntoHBase("htb_weixin", iter)
            case 2 => insertIntoHBase("htb_card", iter)
            case _ => insertIntoHBase("htb_other", iter)
          }
        })
      })

    // TODO: 4. 启动实时流式应用，开始准备接收数据并进行处理
    ssc.start() // 将会启动Receiver接收器，用于接收数据源端的数据
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如认为终止程序，或者程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /**
    * 将JSON格式数据插入到HBase表中，其中表的ROWKEY为orderId（解析JSON格式获取）
    *
    * @param tableName
    *                  表的名称
    * @param iter
    *             迭代器，二元组中Value为存储的数据，在HBase表中列名为info:value
    */
  def insertIntoHBase(tableName: String, iter: Iterator[(String, String)]): Unit ={

    // a. 获取HBaseConnection
    val hbaseConf = HBaseConfiguration.create()
    val conn: Connection = ConnectionFactory.createConnection(hbaseConf)

    // b. 获取 表的句柄
    val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    // c. 迭代数据插入
    import java.util
    val puts = new util.ArrayList[Put]()
    iter.foreach{ case (_, jsonValue) =>
      // 获取JSON，获取RowKey
      val rowkey = JSON.parseObject(jsonValue).getString("orderId")

      // 创建Put对象
      val put = new Put(Bytes.toBytes(rowkey))

      // 添加列
      put.addColumn(
        HBASE_ETL_TABLE_FAMILY_BYTES,
        HBASE_ETL_TABLE_COLUMN_BYTES,
        Bytes.toBytes(jsonValue)
      )

      // 将put加入到List中
      puts.add(put)
    }

    // c. 批量插入数据到HBase表中
    table.put(puts)

    // d. 关闭连接
    conn.close()
  }

}
