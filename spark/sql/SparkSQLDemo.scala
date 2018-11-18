package com.erongda.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.sql._

/**
  * 演示SparkSQL读取各种数据源的数据，进行分析
  */
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {

    // TODO：1. 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName("SparkSQLDemo")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    // From implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // TODO: 获取SparkContext实例对象
    val sc = spark.sparkContext
    // 设置日志级别
    sc.setLogLevel("WARN")

    /**
      * TODO: 读取存储在HDFS上parquet格式数据
      */
    val usersDF: DataFrame = spark.read.parquet("/datas/resources/users.parquet")
    // spark.read.format("parquet").load("/datas/resources/users.parquet")
    // 查看Schema信息
    usersDF.printSchema()
    // 显示样本数
    usersDF.show(5, truncate = false)

    println("===========================================")

    // TODO: 对于SparkSQL来说，默认情况下，load加载的数据格式为列式存储的parquet格式文件
    val loadDF = spark.read.load("/datas/resources/users.parquet")
    loadDF.printSchema()
    loadDF.show(5, truncate = false)

    println("================= 从MySQL数据库表中读取数据 ==========================")


    /**
      * 从MySQL数据库中读取数据：销售订单表数据 so
      *     def jdbc(url: String, table: String, properties: Properties): DataFrame
      */
    // 连接数据库URL
    val url: String = "jdbc:mysql://bigdata-training01.erongda.com:3306/"
    // JDBC database connection arguments
    val props: Properties = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    // TODO：从数据库表读取数据
    val soDF: DataFrame = spark.read.jdbc(url, "order_db.so", props)

    // 增加Where条件
    // val predicates: Array[String] = Array("order_amt >= 100")
    // val soDF: DataFrame = spark.read.jdbc(url, "order_db.so", predicates, props)

    // 样本数据和Schema信息
    println(s"Count = ${soDF.count()}")
    soDF.printSchema()
    soDF.show(5, truncate = false)

    println("================= 对封装在DataFrame中数据分析 ==========================")

    /**
      * SparkSQL中分析数据：
      *   -1. SQL 分析： 最原始提供，类似HiveQL
      *   -2. DSL 分析：调用DataFrame/Dataset API
      *
      *  Hive中包含很多自带函数，SparkSQL同样支持
      *     org.apache.spark.sql.functions
      */
    // TODO: DSL 分析，调用API
    import org.apache.spark.sql.functions._
    // 统计所有的订单销售额
    soDF
      // 要选取分析的字段
      .select($"order_amt")
      // 对字段进行聚合统计
      // .agg("order_amt" -> "sum")
      .agg(sum("order_amt"))
      // 某个字段重命名
      .withColumnRenamed("sum(order_amt)", "sum_amt")
      // 调用内置函数，进行保留两位有效数字
      .selectExpr("round(sum_amt, 2) as sum_amt")
      .show()

    println("---------------------------------------------------")

    // TODO：使用SQL分析
    // a. 将DataFrame注册为一张临时视图（相当于数据库中视图），视图中数据仅仅只读
    soDF.createOrReplaceTempView("view_tmp_so")

    // b. 编写SQL分析
    val sumAmtDF = spark.sql(
      """
        |SELECT ROUND(SUM(order_amt), 2) AS sum_amt FROM view_tmp_so
      """.stripMargin)
    sumAmtDF.show()


    /**
      * 使用DSL分析并保存结果到MySQL表中
      */
    val userOrderCountDF: Dataset[Row] = soDF
      .select($"date", $"user_id", $"order_amt")
      // 订单金额 大于 50 的订单数据
      .filter($"order_amt" > 50)
      // 按照 用户 分组, 使用count函数以后，字段名称$"order_amt"
      .groupBy($"user_id").count()
      // 过滤，每个用必须是多个订单， count >= 2
      .where($"count" >= 2)
      // 按照 count 进行降序排序
      .sort($"count".desc)
      // 只获取前5个信息
      .limit(5)

    userOrderCountDF.show()

    // TODO: 将结果保存到MySQL数据库中
    userOrderCountDF
      .write
      .mode(SaveMode.Append)
      .jdbc(url, "test.user_order_count", props)

    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
