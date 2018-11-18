package com.erongda.bigdata.spark.logs

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * 针对Apache Log日志使用SparkSQL进行分析
  */
object SQLLogAnalyzerSpark {

  def main(args: Array[String]): Unit = {
    // TODO：1. 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SQLLogAnalyzerSpark")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // From implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // TODO: 读取HDFS上日志文件
    val apacheLogsRDD: RDD[ApacheAccessLog] = spark.read
      .textFile(ContantUtils.LOCAL_DATA_DIC + "/access_log")
      .rdd   // 将DataFrame转换为RDD
      .filter(ApacheAccessLog.isValidateLogLine)
      .map(line => ApacheAccessLog.parseLogLine(line))

    /**
      * TODO: 如何将RDD转换为DataFrame/Dataset 呢？？？
      *     DataFrame/Dataset = RDD + Schema
      *
      *   方式：
      *      RDD[CaseClass] 可以通过反射推断得到Schame信息
      */
    val accessLogsDS: Dataset[ApacheAccessLog] = apacheLogsRDD.toDS() // 通过隐式转换

    // 查看Schame信息
    accessLogsDS.printSchema()
    /**
      *root
           |-- ipAddress: string (nullable = true)
           |-- clientIdented: string (nullable = true)
           |-- userId: string (nullable = true)
           |-- dateTime: string (nullable = true)
           |-- method: string (nullable = true)
           |-- endpoint: string (nullable = true)
           |-- protocol: string (nullable = true)
           |-- responseCode: integer (nullable = false)
           |-- contentSize: long (nullable = false)
      */
    accessLogsDS.show(5, truncate = false)

    // TODO: 由于后续分析多次，将Dataset将缓存
    accessLogsDS.persist(StorageLevel.MEMORY_AND_DISK_2)

    /**
      * TODO：SparkSQL中数据分析有两种方式：
      *   -a. SQL分析
      *     将DataFrame/Dataset转换为临时视图
      *   -b. DSL分析
      *     调用DataFrame/Dataset中API即可
      */
    // 注册为临时视图
    accessLogsDS.createOrReplaceTempView("view_tmp_access_log")
    // 测试，查询视图中条目数
    spark.sql("SELECT COUNT(1) AS cnt FROM view_tmp_access_log").show()


    /**
      * 需求一：Content Size
      *     The average, min, and max content size of responses returned from the server
      */
    val contentSizeDF: DataFrame = spark.sql(
      """
        |SELECT
        |  AVG(contentSize) AS avg_cs, MIN(contentSize) AS min_cs, MAX(contentSize) AS max_cs
        |FROM
        |  view_tmp_access_log
      """.stripMargin)
    val contentSizeStateRow = contentSizeDF.first()
    println(s"Content Size: Avg -> ${contentSizeStateRow(0)}, Min -> ${contentSizeStateRow(1)}, Max -> ${contentSizeStateRow(2)}")


    /**
      * 需求二：Response Code
      *     A count of response code's returned.
      */
    spark.sql(
      """
        |SELECT
        |  responseCode, COUNT(1) AS total
        |FROM
        |  view_tmp_access_log
        |GROUP BY
        |  responseCode
        |ORDER BY
        |  total DESC
      """.stripMargin).show(20, truncate = false)

    println("----------------- DSL 分析-----------------")
    accessLogsDS
      .select($"responseCode")
      .groupBy($"responseCode").count()
      .orderBy($"count".desc).limit(20)
      .show(20, truncate = false)

    /**
      * 需求三：IP Address
      *     All IP Addresses that have accessed this server more than N times.
      *  思路：
      *     -a. 统计每个IP地址访问服务器的总次数
      *     -b. 过滤访问次数大于某个值的所有IP地址
      */
    accessLogsDS //
      .select($"ipAddress") // 选取字段
      .groupBy($"ipAddress").count() // 分组，统计
      .filter($"count" > 30) // 过滤
      .show(truncate = false)

    println("================================================")

    spark.sql(
      """
        |SELECT ipAddress, COUNT(1) AS cnt FROM view_tmp_access_log GROUP BY ipAddress HAVING cnt > 30
      """.stripMargin).show(truncate = false)


    /**
      * 需求四：Endpoint
      *     The top endpoints requested by count.
      */
    accessLogsDS
      .select($"endpoint")
      .groupBy($"endpoint").count()
      .sort($"count".desc).limit(10)
      .show(truncate = false)

    println("================================================")

    spark.sql(
      """
        |SELECT endpoint, COUNT(1) AS cnt FROM view_tmp_access_log GROUP BY endpoint ORDER BY cnt DESC LIMIT 10
      """.stripMargin)
      .show(truncate = false)

    // 释放缓存Dataset
    accessLogsDS.unpersist()

    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
