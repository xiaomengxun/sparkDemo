package com.erongda.bigdata.spark.sql

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 在SparkSQL中提供读取类似CSV、TSV格式数据，基本使用说明
  */
object SparkCSvSQL {

  def main(args: Array[String]): Unit = {

    // TODO：构建SparkSession实例对象
    val spark = SparkSession
      .builder()
      .appName("SparkCSvSQLExample")
      .master("local[4]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    /**
      * 实际企业数据分析中
      *     csv\tsv格式数据，每个文件的第一行（head, 首行），字段的名称（列名）
      */

    // TODO: 读取TSV格式数据
    val mlRatingsDF: DataFrame = spark.read
      .option("sep", "\t") // 设置每行数据各个字段之间的分隔符， 默认值为 逗号
      .option("header", "true")  // 设置数据文件首行为列名称，默认值为 false
      .option("inferSchema", "true")  // 自动推荐数据类型，默认值为false
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.dat")

    println("=======================================================")

    // 定义Schema信息
    val schema = StructType(
      StructField("user_id", IntegerType, nullable = true) ::
      StructField("movie_id", IntegerType, nullable = true) ::
      StructField("rating", DoubleType, nullable = true) ::
      StructField("timestamp", StringType, nullable = true) :: Nil
    )

    // TODO: 读取TSV格式数据
    val mlRatingsDF2: DataFrame = spark.read
      .option("sep", "\t") // 设置每行数据各个字段之间的分隔符， 默认值为 逗号
      .schema(schema)  // 指定Schema信息
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.data")

    mlRatingsDF2.printSchema()
    mlRatingsDF2.show(5, truncate = false)


    println("=======================================================")

    /**
      * 将电影评分数据保存为CSV格式数据
      */
    mlRatingsDF2.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .option("sep", ",")
      .option("header", "true") // TODO: 建议设置首行为列名
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/ml.csv")


    // 关闭资源
    spark.stop()
  }

}
