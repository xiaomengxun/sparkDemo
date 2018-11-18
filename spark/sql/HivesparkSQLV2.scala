package com.erongda.bigdata.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 集成 Hive表读取表中的数据，进行分析, 在pom.xml中添加如下依赖：
  *     <dependency>
            *<groupId>org.apache.spark</groupId>
            *<artifactId>spark-hive_2.11</artifactId>
            *<version>2.2.0</version>
        *</dependency>
  */
object HivesparkSQLV2 {

  def main(args: Array[String]): Unit = {

    // TODO: 构建SparkSession实例对象
    val spark = SparkSession
      .builder()
      .appName("HivesparkSQL")
      .master("local[3]")
      // 告知SparkSession与Hive集成
      .enableHiveSupport()
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    /**
      * 读取Hive表中的数据进行分析
      */
    spark
      .sql(
        """
          |SELECT deptno, ROUND(AVG(sal), 2) AS avg_sal FROM default.emp GROUP BY deptno
        """.stripMargin)
      .show()

    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
