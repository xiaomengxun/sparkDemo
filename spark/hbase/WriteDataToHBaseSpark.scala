package com.erongda.bigdata.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将RDD的数据保存到HBase表中，使用SparkCore中API完成
  */
object WriteDataToHBaseSpark {

  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("SparkGroupSort")
      .setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(config)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    // sc.setLogLevel("WARN")

    /**
      * 模拟数据：
      *   将词频统计的结果存储到HBase表中，
      *   设计表：
      *     表的名称：ht_wordcount
      *     RowKey：word
      *     列簇：info
      *     列：count
      */
    // 创建Scala中集合类列表List
    val list = List(("hadoop", 234), ("spark", 3454), ("hive", 343434), ("ml", 8765))
    // 通过并行化集合创建RDD
    val wordcountRDD: RDD[(String, Int)] = sc.parallelize(list, numSlices = 1)


    /**
      * TableOutputFormat向HBase表中写入数据，要求（Key， Value），所以要转换数据类型：
      *   RDD[(ImmutableBytesWritable, Put)]
      */
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = wordcountRDD.map{
      case (word, count) =>
        // RowKey
        val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))
        // 创建Put对象
        val put = new Put(rowKey.get())
        // 增加列
        put.addColumn(
          Bytes.toBytes("info"), // cf
          Bytes.toBytes("count"), // column
          Bytes.toBytes(count.toString) // value
        )
        // 返回二元组
        (rowKey, put)
    }

    // TODO: 读取配置信息
    val conf: Configuration = HBaseConfiguration.create()

    // a. 设置数据保存的表名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "ht_wordcount")
    // b. 设置输出格式OutputFormat
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    // c. 设置输出路径
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/datas/spark/hbase/htwc_" + System.currentTimeMillis())

    // def saveAsNewAPIHadoopDataset(conf: Configuration): Unit
    putsRDD.saveAsNewAPIHadoopDataset(conf)

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
