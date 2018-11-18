package com.erongda.bigdata.spark.meituan.product

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.erongda.bigdata.spark.meituan.mock.MockDataUtils
import com.erongda.bigdata.spark.meituan.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * 基于SparkSQL实现Top10各区域热门商品统计分析，并将结果 保存 MySQL数据库表中
  */
object AreaTop10ProductSparkV2 {

  /**
    * 按照给定条件进行数据过滤，获取用户行为信息表数据: user_visit_action
    *     依据需求仅需字段：click_product_id、city_id
    * @param spark
    *              SparkSession 会话实例对象
    * @param startDateOption
    *                        过滤数据开始日期可选项
    * @param endDateOption
    *                      过滤数据结束日期可选项
    * @return
    *         DataFrame数据集
    */
  def getActionDataFrameByFilter(spark: SparkSession, startDateOption: Option[String],
                                 endDateOption: Option[String]): DataFrame = {
    // 由于数据存储Hive表，SparkSQL集成Hive，直接使用SQL查询数据
    /**
      * 各区域热门商品Top分析，数据中有搜索日志数据、有订单相关的日主数据，所以过滤获取仅仅是点击页面的数据
      *     click_product_id 不能为null, 为空，为空字符串
      */
    // a. 构建SQL语句
    val sql =
      s"""
        |SELECT
        | va.click_product_id, va.city_id
        |FROM
        |  db_track.visit_action va
        |WHERE
        |  isNotEmpty(va.click_product_id)
        |  ${startDateOption.map(date => s"AND va.date_part >= '$date'").getOrElse("")}
        |  ${endDateOption.map(date => s"AND va.date_part <= '$date'").getOrElse("")}
      """.stripMargin
    // println(s"==================================================================$sql")

    // b. 执行SQL并返回DataFrame
    spark.sql(sql)
  }

  /**
    * 获取保存在RDBMS中的城市信息数据，并形成DataFrame返回
    *   实现方式：
    *     1. 通过自定义InputFormat，spark
    *     2. 通过SparkSQL的read编程模型，SparkSQL中read数据读取器 提供读取RDBMS表的数据
    * @param spark
    *              SparkSession 会话实例对象
    * @return
    *         DataFrame数据集
    */
  def getCityInfoDataFrame(spark: SparkSession): DataFrame = {
    // 1. 根据相关配置信息获取jdbc的连接信息
    val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/db_meituan?characterEncoding=UTF-8"
    val tableName = "city_info"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")

    // 2. 构建DataFrame并返回
    val df = spark.read.jdbc(url, tableName, props)
    // df.cache()

    // 3. 返回
    df
  }


  /**
    * 合并两个DataFrame的数据，并将合并后的数据保存为临时视图
    *
    * @param spark
    *              SparkSession 会话实例对象
    * @param action
    *                        用户行为信息数据，必须包含：city_id和click_product_id
    * @param cityInfo
    *                          城市信息数据：必须包含:city_id 和 city_name、area三个字段
    */
  def generateTempProductBasicView(spark: SparkSession, action: DataFrame,
                                   cityInfo: DataFrame): Unit = {
    /**
      * 考虑一下，如果不适用DataFrame的DSL语言是否有其他方式？？
      *   -a. DSL 编程
      *   -b. 将DataFrame转换为RDD，RDD进行Join操作，JOIN以后的结果转换为DataFrame，并注册为临时视图
      *   -c. 将DataFrame注册成为临时视图，然后同HQL语句进行数据Join操作
      */

    // 0. 隐式导入
    import spark.implicits._

    // 1. 数据的JOIN
    // 下面的方式函数使用，必须当两个DataFrame中存在相关字段的时候（名称），而且又是按照相同字段进行join的时候，才可以使用
    // action.join(cityInfo)
    val joinDF: DataFrame = action.join(cityInfo, "city_id")
/*
    val joinDF: DataFrame = action
      .toDF("cid", "click_product_id")
      .join(cityInfo, $"cid" === $"city_id", "left")
*/

    // 2. 对数据进行提取操作
    val selectDF = joinDF
      .select($"city_id", $"click_product_id", $"city_name", $"area")
    // selectDF.show(10, truncate = false)

    // 3. 注册为临时视图
    selectDF.createOrReplaceTempView("view_tmp_product_basic")
  }

  /**
    * 从临时视图view_tmp_product_basic中读取数据，并聚合结构得到各个区域 各个商品的点击次数
     * @param spark
    *              SparkSession会话对象
    */
  def generateTempAreaProductCountView(spark: SparkSession): Unit = {

    /**
      * 各个区域 、各个商品的点击次数？？
      *     各个区域是否需要包含具体的城市地址信息（city_id和city_name)
      *  eg:
      *     华东区 101 上海
      *     华东区 102 杭州
      *     华东区 103 苏州
      *     ......
      *  期望的结果：
      *     华东区 101:上海:number,102:杭州:numberr,103:苏州:number ....  total_number
      *  TODO: 仅上述分析，需要定义两个函数完成功能，一个UDF：合并city_name和city_id；一个UDAF，进行area聚合统计
      *
      *  SparkSQL中自定义函数来说，支持两类
      *     - UDF: 一个输入一个输出，普通函数
      *     - UDAF：一组输入，一个输出，用于group by 情况函数
      *
      *  此处数据聚合分为两步走来做：
      *   -a. 将id和name 合并 => UDF 函数完成
      *     concat_int_string(city_id, city_name)
      *   -b. 将合并之后的数据按照 area分组后，进行数据的聚合操作：
      *       一个区域有多个城市，每个个城市可能出现多次同个商品被点击，需要计算次数，最终合并区域中同个商品在不容城市被点击的次数
      *       group_concat_distinct(concat_string)
      */
    // 1. 构建SQL语句
    val sql =
      """
        |SELECT
        |  area, click_product_id,
        |  COUNT(1) AS click_count,
        |  group_concat_distinct(concat_int_string(city_id, city_name)) AS city_infos
        |FROM
        |  view_tmp_product_basic
        |GROUP BY
        |  area, click_product_id
      """.stripMargin

    // 2. 执行SQL语句，得到DataFrame
    val df = spark.sql(sql)
    // df.show(40, truncate = false)

    // 3. 将DataFrame注册成为临时视图
    df.createOrReplaceTempView("view_tmp_area_product_count")
  }


  /**
    * 从临时视图 view_tmp_area_product_count获取各个区域访问次数最多的前10个商品
    *   - 分组TopKey的程序
    *   - 在SparkSQL中使用row_number来实现
    *
    * @param spark
    */
  def generateTempAreaTop10ProductCountView(spark: SparkSession): Unit ={
    // a. 构建SQL语句
    val sqlStr: String =
      """
        |SELECT
        |  tmp.area, tmp.click_product_id, tmp.click_count, tmp.city_infos
        |FROM(
        |  SELECT
        |    area, click_product_id, click_count, city_infos,
        |    ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) AS rnk
        |  FROM
        |    view_tmp_area_product_count
        |) AS tmp
        |WHERE
        |  tmp.rnk <= 10
      """.stripMargin

    // b. 获取DataFrame对象，执行SQL语句
    val df = spark.sql(sqlStr)
    // df.show(40, truncate = false)

    // c. 将DataFrame注册为临时视图，后续与商品信息表进行关联分析
    df.createOrReplaceTempView("view_tmp_area_top10_product_count")
  }

  /**
    *   将临时视图view_tmp_area_top10_product_count与Hive中商品信息表product_info进行数据关联操作，
    * 最终将关联结果注册为临时视图。
    * @param spark
    *              SparkSession会话上下文对象
    */
  def generateTempAreaTop10CountFullProductView(spark: SparkSession): Unit ={
    /**
      * - 关联商品信息表数据，需要解析商品扩展信息字段数据，判断商品属于第三方商品还是自营商品
      *   - 0: 自营商品
      *   - 1： 第三方商品
      *   解析JSON函数UDF：get_string_from_json(json_str, type)
      * - 数据存储在字段extend_info中, 数据格式为JSON格式 => 解析获取数据，并判断数据
      *     使用if函数进行数据判断操作，语法为：
      *        if(condition, true-value, false_value): 当条件为true的数，返回值为true-value; 当condition为fasle时，返回值为false-value.
      */
    /**
      * area: 华东、华南、华中、华北、西北、西南、东北
      * 地域级别划分为：
      *     - 华东、华南、华北 ==> A
      *     - 东北 ==> B
      *     - 华中、西南 ==> C
      *     - 西北 => D
      *     - 其他 => E
      *  使用多条件case...when...then...语句
      */
    // a. 构建SQL语句
    val sqlStr: String =
      """
        |SELECT
        |  vtatpc.area,
        |  CASE
        |    WHEN vtatpc.area = "华东" OR vtatpc.area = "华南" OR vtatpc.area = "华北" THEN "A"
        |    WHEN vtatpc.area = "东北" THEN "B"
        |    WHEN vtatpc.area = "华中" OR vtatpc.area = "西南" THEN "C"
        |    WHEN vtatpc.area = "西北" THEN "D"
        |    ELSE "E"
        |  END AS area_level,
        |  vtatpc.click_product_id AS product_id,
        |  vtatpc.city_infos,
        |  vtatpc.click_count,
        |  pi.product_name,
        |  if(get_string_from_json(pi.extend_info, "product_type") == "0", "自营商品", "第三方商品") AS product_type
        |FROM
        |  view_tmp_area_top10_product_count vtatpc
        |JOIN
        |  db_track.product_info pi
        |ON
        |  vtatpc.click_product_id = pi.product_id
      """.stripMargin
    // b. 执行SQL语句
    val df = spark.sql(sqlStr)
    // df.printSchema()
    // df.show(40, truncate = false)

    // c. 注册成为临时视图
    df.createOrReplaceTempView("view_tmp_area_top10_full_product")
  }


  /**
    * 将临时视图view_tmp_area_top10_product_count中数据持久化到关系型数据库表汇总
    * @param spark
    *              SparkSession会话实例对象
    */
  def persistAreaTop10CountFullProductData(spark: SparkSession, taskId: Long): Unit ={
    /**
      * DataFrame数据写出到RDBMs主要有两种方式：
      *   -a. 直接调用DataFrame中write编程模型将数据写出（jdbc接口，eg：df.write.jdbc)
      *   -b. 将DataFrame转换为RDD，RDD数据输出到RDBMS:
      *     使用foreachPartition函数或者自定OutputFormat（调用saveAsNewAPIHadoopDataset)
      */
    // TODO: 此处采用第二种方式数据输出持久化，原因是程序输出数据需要实现Insert or Update的操作
    // Insert or Update：当不存在的时候进行插入操作，当存在的时候进行更新操作（存放与否依据主键进行判断）
    // a. 读取临时视图数据形成DataFrame
    val df: DataFrame = spark.sql(
      """
        |SELECT
        |  area, area_level, product_id, city_infos, click_count, product_name, product_type
        |FROM
        |  view_tmp_area_top10_full_product
      """.stripMargin)
    // b. 转换为RDD然后调用foreachPartition API进行数据输出 => 可以直接调用DataFrame的foreachPartition
    df.foreachPartition(iter => {
      Try{
        // i. 获取conncetion
        val conn: Connection = {
          // TODO: a. Create DB Connection
          Class.forName("com.mysql.jdbc.Driver")
          // 连接数据库三要素
          val url = "jdbc:mysql://bigdata-training01.erongda.com:3306/db_meituan?characterEncoding=UTF-8"
          val username = "root"
          val password = "123456"
          // 获取连接并返回
          DriverManager.getConnection(url, username, password)
        }

        // 获取数据库原始事务设置
        val oldAutoCommit = conn.getAutoCommit
        // TODO: 设置事务，针对当前分区数据要么都插入数据库，要么都不插入
        conn.setAutoCommit(false)

        // ii. 创建PreparedStatement 实例对象
        // 插入数据的SQL语句
        /**
          * 此处要实现的功能：Insert or Update，针对MySQL数据库来说语法如下：
          *     INSERT INTO tableName(field1, field2, ...) VALUES (?, ?, ...) ON DUPLICATE KEY UPDATE field1=value1, field2=value2, ...
          */
        val sqlStr = "INSERT INTO tb_area_top10_products(`task_id`, `area`, `product_id`, `area_level`, `count`, `city_infos`, `product_name`, `product_type`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `area_level` = VALUES(`area_level`), `count` = VALUES(`count`),`city_infos` = VALUES(`city_infos`),`product_name` = VALUES(`product_name`),`product_type` = VALUES(`product_type`)"
        val pstmt: PreparedStatement =  conn.prepareStatement(sqlStr)

        // iii. 对数据进行迭代输出操作
        // 定义计数器，计数数据条目数
        var recordCount = 0
        iter.foreach(row => {
          val area = row.getAs[String]("area")
          val areaLevel = row.getAs[String]("area_level")
          val productId = row.getAs[String]("product_id")
          val cityInfos = row.getAs[String]("city_infos")
          val clickCount = row.getAs[Long]("click_count")
          val productName = row.getAs[String]("product_name")
          val productType = row.getAs[String]("product_type")

          // 设置参数
          pstmt.setLong(1, taskId)
          pstmt.setString(2, area)
          pstmt.setString(3, productId)
          pstmt.setString(4, areaLevel)
          pstmt.setLong(5, clickCount)
          pstmt.setString(6, cityInfos)
          pstmt.setString(7, productName)
          pstmt.setString(8, productType)

          // 添加批次
          pstmt.addBatch()
          recordCount += 1

          // 设置每批次提交的数据量为500，当达到500条数据的时候，提交执行一次
          if(recordCount % 500 == 0){
            pstmt.executeBatch()
            conn.commit()
          }
        })

        // iv. 进行批次提交事务，插入数据
        pstmt.executeBatch()
        // 手动提交事务，将批量数据要么全部插入，要么不全不插入失败
        conn.commit()

        // 返回值
        (oldAutoCommit, conn)
      }match {
        // 执行成功，没有异常
        case Success((oldAutoCommit, conn)) => {
          // 当数据插入成功到数据库以后，恢复原先数据库事务相关设置
          Try(conn.setAutoCommit(oldAutoCommit))
          if(null != conn) conn.close()
        }
        case Failure(exception) => throw exception
      }
    })
  }


  def main(args: Array[String]): Unit = {

    // TODO：零、获取参数
    // 从args中获取传入的taskId的值（实际中每个人运行这应用都产生一个唯一标识符ID）
    val taskId = ParamUtils.getTaskIdFromArgs(args)
    if(-1L == taskId){
      throw new IllegalArgumentException(s"参数异常，无法解析task id: ${args.mkString("[", ",", "]")}")
    }

    // TODO: 一、创建SparkSession会话实例对象
    // 1.1 构建SparkSession和SparkContext实例对象
    val (spark, sc) = {
      // i. 获取Spark Application必要的属性
      val appName = TrackConstants.SPARK_APP_NAME_PRODUCT + taskId
      val isLocal = ConfigurationManager.getBoolean(TrackConstants.SPARK_LOCAL)

      // ii. 创建SparkConf上下配置对象
      val sparkConf = SparkConfUtils.generateSparkConf(appName, isLocal)

      // 获取是否与Hive集成
      val isIntegratedHive = ConfigurationManager.getBoolean(TrackConstants.INTEGRATED_HIVE)

      // iii. 创建SparkSession实例对象
      val sparkSession: SparkSession = SparkSessionUtils.getSparkSession(
        conf = sparkConf, integratedHive = isIntegratedHive)

      // TODO：由于此模块处理的数据 在Hive表中，如果不集成Hive的话， 开发的时候需要自己模型加载数据，保存为表
      if(!isIntegratedHive){
        // 加载用户访问表的数据
        MockDataUtils.loadUserVisitActionMockData(sparkSession)
        // 加载商品信息表的数据
        MockDataUtils.loadProductInfoMockData(sparkSession)
      }

      // iv. SparkContext实例对象
      val sparkContext: SparkContext = sparkSession.sparkContext

      // v. 返回对象
      (sparkSession, sparkContext)
    }
    // 设置日志级别
    sc.setLogLevel("WARN")

    // 1.2 将SparkSession的隐式转换导入

    // 1.3 自定义函数UDFS定义与注册和UDAF注册
    // 函数一：判断字符串是否为空
    spark.udf.register(
      "isNotEmpty", //
      (str: String) => {
        !(str == null || "".equals(str.trim) || "null".equalsIgnoreCase(str.trim) )
      }
    )
    // 函数二：合并城市ID和城市Name
    spark.udf.register(
      "concat_int_string", //
      (id: Int, name: String) => s"$id:$name"
    )
    // 函数三：自定义UDAF，统计各个区域对应各个省份 商品的点击次数
    spark.udf.register("group_concat_distinct", GroupConcatDistinctUDAF)
    // 函数四：解析JSON格式数据
    spark.udf.register(
      "get_string_from_json", //
      (json: String, field: String) => {
        JSON.parseObject(json).getString(field)
      }
    )

    // TODO: 二、业务代码编写，分析处理存储
    // TODO: 2.1 执行参数过滤，获取过滤后的用户行为数据
    // a. SparkSQL集成Hive： 启动MetaStore服务，将配置配置放于resources目录或者直接设置
    // b. 此处将过滤参数进行简化，只有date_str日期字段过滤
    val startDateOption: Option[String] = Some("2017-03-11")
    val endDateOption: Option[String] = Some("2017-03-11")
    val actionDataFrame = getActionDataFrameByFilter(spark, startDateOption, endDateOption)
    // actionDataFrame.show(10, truncate = false)

    // TODO: 2.2 读取RDBMs中城市信息，形成城市信息数据
    // a. SparkSQL读取外部数据源MySQL数据库
    val cityInfoDataFrame =  getCityInfoDataFrame(spark)
    // cityInfoDataFrame.show(10, truncate = false)

    // TODO: 2.3 将用户行为数据和城市信息数据进行join操作，并且注册为临时视图
    // a. 不同源的数据直接进行JOIN分析
    generateTempProductBasicView(spark, actionDataFrame, cityInfoDataFrame)

    // TODO: 2.4 统计各个区域各个商品的点击次数，并注册为临时视图
    // a. 数据聚合（UDF和UDAF）
    generateTempAreaProductCountView(spark)

    // TODO: 2.5 获取各个区域Top10的点击数据，并注册成为临时视图
    // a. 分组排序TopKey => 窗口分析函数row_number使用
    generateTempAreaTop10ProductCountView(spark)

    // TODO: 2.6 将Top10结果数据和商品表进行关联，得到具体的商品信息，并注册为临时视图
    // if 函数 和 依据区域得到区域 级别 case...when...
    generateTempAreaTop10CountFullProductView(spark)

    // TODO: 2.7 持久化保存在临时视图中结果数据到关系型数据库汇总
    // DataFrame户数写入到JDBC如何实现
    persistAreaTop10CountFullProductData(spark, taskId)

    Thread.sleep(100000000L)
    spark.stop()
  }

}
