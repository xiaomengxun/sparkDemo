package com.erongda.bigdata.spark.sql

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * 在SparkSQL中提供读取类似CSV、TSV格式数据，基本使用说明
  */
object SparkCSvSQL2 {

  def main(args: Array[String]): Unit = {

    // TODO：构建SparkSession实例对象
    val spark = SparkSession
      .builder()
      .appName("SparkCSvSQL2")
      .master("local[4]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // TODO: 读取文本数据
    val rawRatingsDS: Dataset[String] = spark.read
      .textFile(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-1m/ratings.data")
      .map(line => line.replace("::", ","))

    //
    val ratingsDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rawRatingsDS)
    ratingsDF.printSchema()
    ratingsDF.show(5,truncate = false)

    // 关闭资源
    spark.stop()
  }

}
