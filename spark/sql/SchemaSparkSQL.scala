package com.erongda.bigdata.spark.sql

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 演示SparkSQL自定义Schema信息，将RDD转换为DataFrame
  */
object SchemaSparkSQL {

  def main(args: Array[String]): Unit = {

    // TODO: 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SchemaSparkSQL").master("local[4]")
      .getOrCreate()
    import spark.implicits._

    // 获取SparkContext实例对象
    val sc = spark.sparkContext
    // 设置日志级别
    sc.setLogLevel("WARN")

    // TODO: 使用spark读取文本文件
    val rawRatingsRDD: RDD[String] = spark.read
      .textFile(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-1m/ratings.dat")
      .rdd
    rawRatingsRDD.take(5).foreach(println)

    /**
      * TODO: 采用自定义Schema方式将RDD转换为DataFrame
      */
    // a. RDD[Row]
    val rowRatingsRDD: RDD[Row] = rawRatingsRDD.map(line => {
      val Array(userId, movieId, rating, timestamp) = line.split("::")
      // 构建Row实例对象并返回
      Row(userId.toInt, movieId.toInt, rating.toDouble, timestamp.toLong)
    })

    // b. 定义schema信息
    val ratingSchema: StructType = StructType(
      Array(
        StructField("userId", IntegerType, nullable = true),
        StructField("movieId", IntegerType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    // c. 调用SparkSession中方法转换
    val ratingsDF: DataFrame = spark.createDataFrame(rowRatingsRDD, ratingSchema)

    ratingsDF.printSchema()
    ratingsDF.show(5, truncate = false)

    println("==============================================================")

    /**
      * 使用DSL和SQL分析：
      *     统计每个电影的平均评分，降序排序，要求每部电影的用户评分人数大于100
      */
    import org.apache.spark.sql.functions._
    ratingsDF
      .select($"movieId", $"rating")
      .groupBy($"movieId")
      .agg(count("movieId").alias("cnt"), round(avg("rating"), 2).alias("avg_rating"))
      .filter($"cnt" > 100)
      .orderBy($"cnt".desc, $"avg_rating".desc)
      .limit(20)
      .show(20, truncate = false)

    println("==============================================================")


    // SQL分析
    // a. 将DataFrame注册为临时视图
    ratingsDF.createOrReplaceTempView("view_tmp_ratings")
    // b. 编写SQL分析
    spark.sql(
      """
        |SELECT
        |  movieId, COUNT(movieId) AS cnt, ROUND(AVG(rating), 2) AS avg_rating
        |FROM
        |  view_tmp_ratings
        |GROUP BY
        |  movieId
        |HAVING
        |  cnt > 100
        |ORDER BY
        |  cnt DESC, avg_rating DESC
        |LIMIT
        |  20
      """.stripMargin).show(20, truncate = false)

    // 线程休眠
    Thread.sleep(1000000)
    // 关闭资源
    spark.stop()
  }

}
