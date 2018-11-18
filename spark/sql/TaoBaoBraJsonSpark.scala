package com.erongda.bigdata.spark.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * 使用SparkSQL读取淘宝 文胸评论数据：JSON格式数据，进行分析
  */
object TaoBaoBraJsonSpark {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("TaoBaoBraJsonSpark")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // TODO: 在SparkSQL开发中，通常在创建SparkSession实例对象以后，定义UDF函数并注册
    /**
      * 自定义函数：
      *   目前在SparkSQL中支持udf和UDAF，但是在Hive中定义UDF、UDAF和UDTF函数可以在SparkSQL中使用
      *   需求：
      *      获取size_info中颜色 信息
      *          颜色分类:上薄下厚 酒红;尺码:36/80B
      */
    spark.udf.register(
      "get_bra_color", // 函数名称
      (sizeInfo: String) => {   // 匿名函数
        sizeInfo.split("\\;")(0)
      }
    )

    // TODO: 注册自定义的聚合函数
    spark.udf.register("sal_avg", AvgSalUDAF)


    /**
      * TODO：读取JSON格式BRA评论数据
      */
    val taobaoBraDF: DataFrame = spark.read
      .json("file:///E:/spark-datas/rates.dat")
      .coalesce(20)

    // 数据 基本状况
    taobaoBraDF.printSchema()
    println(s"Count = ${taobaoBraDF.count}")
    taobaoBraDF.show(numRows = 5, truncate = false)

    //
    taobaoBraDF.persist(StorageLevel.MEMORY_AND_DISK)

    /**
      * TODO: 基于SQL分析，使用自定义函数
      */
    // a. 将DataFrame注册为临时视图
    taobaoBraDF.createOrReplaceTempView("view_tmp_taobao_bra")
    // b. 编写SQL分析
    val braColorDF = spark.sql(
      """
        |SELECT
        |  get_bra_color(size_info) AS bra_color
        |FROM
        |  view_tmp_taobao_bra
        |WHERE
        |  length(trim(size_info)) > 0
      """.stripMargin)
    braColorDF.show(50, truncate = false)

    println("===================================================")

    import org.apache.spark.sql.functions._
    taobaoBraDF
      .filter(length(trim($"size_info")) > 0)
      // 使用函数
      .selectExpr("get_bra_color(size_info) AS bra_color")
      .show(50, truncate = false)

    taobaoBraDF.unpersist()
    // 线程休眠
    Thread.sleep(1000000)

    // 关闭资源
    spark.stop()
  }

}
