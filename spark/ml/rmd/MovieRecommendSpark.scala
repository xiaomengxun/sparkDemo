package com.erongda.bigdata.spark.ml.rmd

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * 基于DataFrame API实现的Spark ML中协同过滤算推荐算法ALS进行电影推荐
  */
object MovieRecommendSpark {

  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName("MovieRecommendSpark")
      .master("local[4]")
      .config("spark.sql.shuffle.partition", "4")
      .getOrCreate()
    // 导入隐式转换
    import spark.implicits._

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 定义Schema信息
    val schema = StructType(
      StructField("userId", IntegerType, nullable = true) ::
        StructField("movieId", IntegerType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("timestamp", StringType, nullable = true) :: Nil
    )

    // TODO: 读取TSV格式数据
    val rawRatingsDF: DataFrame = spark.read
      .option("sep", "\t") // 设置每行数据各个字段之间的分隔符， 默认值为 逗号
      .schema(schema)  // 指定Schema信息
      .csv(ContantUtils.LOCAL_DATA_DIC + "/als/movielens/ml-100k/u.data")

    rawRatingsDF.printSchema()
    /*
    root
       |-- userId: integer (nullable = true)
       |-- movieId: integer (nullable = true)
       |-- rating: double (nullable = true)
       |-- timestamp: string (nullable = true)
    */
    rawRatingsDF.show(10, truncate = false)


    // TODO: ALS 算法（模型学习器）实例对象，应用fit函数训练数据得到模型（转换器）
    val als = new ALS()  // def this() = this(Identifiable.randomUID("als"))
      // 设置迭代的最大次数
      .setMaxIter(10)
      // 设置特征数
      .setRank(10)
      // 显式评价
      .setImplicitPrefs(false)
      // 设置 用户ID: 列名,  产品ID: 列名,  评价：列名
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    // TODO: 使用数据训练模型，得到模型学习器
    val mlAlsModel: ALSModel = als.fit(rawRatingsDF)

    // 用户-特征 因子矩阵
    val userFactorsDF: DataFrame = mlAlsModel.userFactors
    userFactorsDF.show(5, truncate = false)
    // 产品-特征 因子矩阵
    val itemFactorsDF: DataFrame = mlAlsModel.itemFactors
    itemFactorsDF.show(5, truncate = false)

    // TODO: 为用户推荐10个电影（产品）
    val userRecs: DataFrame = mlAlsModel.recommendForAllUsers(10)
    //userRecs.show(5, truncate = false)

    // TODO: 为电影推荐10个用户
    val movieRecs: DataFrame = mlAlsModel.recommendForAllItems(10)
    //movieRecs.show(5, truncate = false)


    val inputDS: Dataset[UserMovie] = spark.sparkContext
      .parallelize(List(UserMovie(471, 989))).toDS()
    val predictDF: DataFrame = mlAlsModel
      .setUserCol("userId").setItemCol("movieId").setPredictionCol("predictRating")
      .transform(inputDS)
    predictDF.show()

    // TODO: 模型评估，计算RMSE（均方根误差）
    // a. 使用模型（转换器）对测试数据集进行预测
    val predictRatingDF: DataFrame = mlAlsModel
      .setUserCol("userId").setItemCol("movieId").setPredictionCol("predictRating")
      .transform(rawRatingsDF)
    // predictRatingDF.show(5, truncate = false)

    // b. 模型的评估
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse") // 指定评估指标
      .setLabelCol("rating")
      .setPredictionCol("predictRating")
    val rmse: Double = evaluator.evaluate(predictRatingDF)
    // println(s"Root-mean-square error = $rmse")

    // TODO： 模型保存
    mlAlsModel.save(ContantUtils.LOCAL_DATA_DIC + "/als/mlAlsModel")

    // TODO: 加载模型
    val loadMlAlsModel = ALSModel.load(ContantUtils.LOCAL_DATA_DIC + "/als/mlAlsModel")
    loadMlAlsModel
      .setUserCol("userId").setItemCol("movieId").setPredictionCol("predictRating")
      .transform(inputDS)
      .show()

    // 线程休眠
    Thread.sleep(10000000)

    // 关闭资源
    spark.stop()
  }

}



case class UserMovie(userId: Int, movieId: Int)
