package com.erongda.bigdata.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现从HDFS读取数据，进行词频统计WordCount，并且将数据写入到MySQL数据库中
  */
object SparkWordCountToMySQL {

  /**
    * Scala 语言中程序的入口
    * @param args
    *             程序运行传递的参数
    */
  def main(args: Array[String]): Unit = {

    /**
      * 构建SparkContext实例对象，用于读取要处理的数据及调度应用执行
      */
    // TODO: 设置Spark Application的配置信息，比如应用的名称、应用运行地方
    val config: SparkConf = new SparkConf()
      .setAppName("SparkWordCountToMySQL")
    //  .setMaster("local[2]")
    // 创建SparkContext 实例对象
    val sc = SparkContext.getOrCreate(config)
    // 设置日志级别  Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

    /**
      * 第一步、读取数据，从HDFS读取数据
      */
    val inputRDD: RDD[String] = sc.textFile("/datas/wordcount.data")

    // 样本数据和条目数
    println(s"Count = ${inputRDD.count()}")
    println(s"First: \n\t${inputRDD.first()}")

    /**
      * 第二步、数据的分析（调用RDD中转换函数）
      */
    val wordCountRDD: RDD[(String, Int)] = inputRDD
      // 对每一条数据进行分割为单词
      .flatMap(line => line.split("\\s+"))
      // 将单词转换为二元组形式
      .map(word => (word, 1))
      // 按照Key（单词）分组，聚合统计出现的总次数
      .reduceByKey((a, b) => a + b)

    /**
      * 第三步、数据的保存（调用Action数据，更多的是使用foreach）
      */
    // wordCountRDD.foreach(tuple => println(s"word = ${tuple._1}, count = ${tuple._2}"))

    // TODO: 将数据保存到MySQL数据库中
    wordCountRDD
      // 由于结果RDD中数据较少，分区较多，可以降低分区数
      .coalesce(1)
      // 将结果RDD保存到外部存储系统，比如MySQL数据库或者Redis中或者HBase表中等等
      .foreachPartition(iter => {
        // TODO: a. Create DB Connection
        Class.forName("com.mysql.jdbc.Driver")
        // 连接数据库三要素
        val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/test"
        val username = "root"
        val password = "123456"

        var conn: Connection = null
        try{
          // i. 获取数据库的连接
          conn = DriverManager.getConnection(url, username, password)

          // ii. 构建PreparedStatement实例对象
          val sqlStr = "INSERT INTO rdd_word_count (word, count) VALUES (?, ?)"
          val pstmt: PreparedStatement = conn.prepareStatement(sqlStr)

          // TODO: b. Insert Into Table
          iter.foreach{
            case (word, count) =>
              println(s"word = $word, count = $count")
              // 将数据保存到MySQL数据库中
              pstmt.setString(1, word)
              pstmt.setInt(2, count)

              // 更新插入
              pstmt.executeUpdate()
          }
        }catch {
          case e: Exception => e.printStackTrace()
        }finally {
          // TODO: c. Close Connection
          if(null != conn) conn.close()
        }
      })

    wordCountRDD.saveAsTextFile("")

    // 为了监控显示界面，线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }
}
