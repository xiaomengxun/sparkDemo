package com.erongda.bigdata.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 集成 Hive表读取表中的数据，进行分析, 在pom.xml中添加如下依赖：
  *     <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>
  */
object HivesparkSQL {
  def main(args: Array[String]): Unit = {
    // TODO: 构建SparkSession实例对象
    val spark = SparkSession
      .builder()
      .appName("HivesparkSQL")
      .master("local[3]")
      // 告知SparkSQL，Hive MeteStoreServer服务在哪里
      .config("hive.metastore.uris", "thrift://bigdata-training01.erongda.com:9083")
      // 下面的属性从Spark 2.0开始过时
      // .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir ", "/user/hive/warehouse")
      // 告知SparkSession与Hive集成
      .enableHiveSupport()
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**
      * 创建表 -> 数据库名称：db_0624
      */
    spark.sql(
      """
        |create table emp(
        |empno int,
        |ename string,
        |job string,
        |mgr int,
        |hiredate string,
        |sal double,
        |comm double,
        |deptno int
        |)
        |row format delimited fields terminated by '\t'
      """.stripMargin).show()



    // 线程休眠
    Thread.sleep(100000)

    // 关闭资源
    spark.stop()
  }

}
