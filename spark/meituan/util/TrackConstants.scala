package com.erongda.bigdata.spark.meituan.util

/**
  * Project Basic Constants：项目中常用的常量
  */
object TrackConstants {

  /**
    * Spark Application Constants
    */
  val SPARK_APP_NAME_PRODUCT: String = "AreaTop10ProductSpark_"
  val SPARK_APP_NAME_AD: String = "AdClickRealTimeStateSpark"
  // 是否运行本地模式
  val SPARK_LOCAL: String = "spark.local"


  /**
    * Project Configuration Constants
    */
  val JDBC_DRIVER: String = "jdbc.driver"
  val JDBC_DATASOURCE_SIZE: String = "jdbc.datasource.size"
  val JDBC_URL: String = "jdbc.url"
  val JDBC_USER: String = "jdbc.user"
  val JDBC_PASSWORD: String = "jdbc.password"

  /**
    * Hive MetaStore Constants
    */
  val INTEGRATED_HIVE = "integrated.hive"
  val HIVE_METASTORE_URIS = "hive.metastore.uris"
  val HIVE_METASTORE_WAREHOUSE_DIR = "hive.metastore.warehouse.dir"


  /**
    * Task Constants
    */
  val PARAM_START_DATE: String = "startDate"
  val PARAM_END_DATE: String = "endDate"

  /**
    * 数据库中相关表的名称
    */
  val MYSQL_CITY_INFO_TABLE_NAME = "city_info"

  /**
    * Kafka Constants
    */
  // KAFKA CLUSTER BROKERS
  val METADATA_BROKER_LIST = "metadata.broker.list"
  val TOPIC_NAMES = "kafka.topics"
  val AUTO_OFFSET_RESET = "auto.offset.reset"
  val ZK_CONNECT_URL = "zookeeper.connect.url"

}
