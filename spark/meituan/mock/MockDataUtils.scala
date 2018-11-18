package com.erongda.bigdata.spark.meituan.mock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 模型加载数据
  */
object MockDataUtils {

  val LOCAL_TRACK_DATAS_DIRC = "file:///D:/ProjectWorkspace/RDBD03/spark-learning/datas/track"

  /**
    * date_str, user_id, session_id, page_id, action_time, search_keyword,
    * click_category_id, click_product_id, order_category_ids,
    * order_product_ids, pay_category_ids, pay_product_ids, city_id
    * @param spark
    *              SparkSession会话对象
    */
  // 加载UserVisitAction数据
  def loadUserVisitActionMockData(spark: SparkSession): Unit ={
    // a. 自定义Schema信息
    val userVisitActionSchema: StructType = StructType(
     StructField("date_part", StringType, nullable = true) ::
     StructField("user_id", StringType, nullable = true) ::
     StructField("session_id", StringType, nullable = true) ::
     StructField("page_id", StringType, nullable = true) ::
     StructField("action_time", StringType, nullable = true) ::
     StructField("search_keyword", StringType, nullable = true) ::
     StructField("click_category_id", StringType, nullable = true) ::
     StructField("click_product_id", StringType, nullable = true) ::
     StructField("order_category_ids", StringType, nullable = true) ::
     StructField("order_product_ids", StringType, nullable = true) ::
     StructField("pay_category_ids", StringType, nullable = true) ::
     StructField("pay_product_ids", StringType, nullable = true) ::
     StructField("city_id", StringType, nullable = true) :: Nil
    )
    // b. 使用spark.read.csv读取数据
    val df = spark.read
      .schema(userVisitActionSchema) //
      .csv(s"$LOCAL_TRACK_DATAS_DIRC/user_visit_action")

    // TODO：当数据库不存在的时候，就创建
    spark.sql("CREATE DATABASE IF NOT EXISTS db_track").count()
    df.write
       .partitionBy("date_part")  // 设置保存表的分区字段
       .saveAsTable("db_track.visit_action")
    // spark.read.table("db_track.user_visit_action").show(10, truncate = false)
  }

  // 加载ProductInfo数据
  def loadProductInfoMockData(spark: SparkSession): Unit ={
    // a. 自定义Schema信息
    val productInfoSchema = StructType(
      Array(
        StructField("product_id", StringType, nullable = true),
        StructField("product_name", StringType, nullable = true),
        StructField("extend_info", StringType, nullable = true)
      )
    )

    // b. 使用spark.read 读取数据
    val productInfoDF = spark.read
      .schema(productInfoSchema) //
      .option("sep", "^") //
      .csv(s"$LOCAL_TRACK_DATAS_DIRC/product_info")

      spark.sql("CREATE DATABASE IF NOT EXISTS db_track").count()
      // 保存为表
      productInfoDF.write.saveAsTable("db_track.product_info")
  }
}
