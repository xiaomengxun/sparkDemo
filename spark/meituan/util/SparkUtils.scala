package com.erongda.bigdata.spark.meituan.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  */
object SparkConfUtils {

  /**
    *  根据app名称和是否是本地运行环境返回一个SparkConf对象实例
    * @param appName
    *                App名称
    * @param isLocal
    *                是否是本地运行环境，默认为false
    * @return
    */
  def generateSparkConf(appName: String, isLocal: Boolean = false): SparkConf = {
    // 构建SparkConf实例对象
    val conf = if(isLocal){
      // 本地运行环境，设置Master
      new SparkConf().setAppName(appName).setMaster("local[4]")
    }else{
      // 集群运行环境，不设置master，master参数有启动脚本--master设置
      new SparkConf().setAppName(appName)
    }

    // 设置分区数
    conf.set("spark.sql.shuffle.partitions", "8")


    // 返回创建SparkConf对象
    conf
  }
}

object SparkContextUtils{
  /**
    * 根据给定的SparkConf创建SparkContext对象<br/>
    * 如果JVM中存在一个SparkContext对象，那么直接返回给也已有的，否则创建一个新的SparkContext实例对象
    * @param conf
    *             SparkConf实例对象
    * @return
    */
  def getSparkContext(conf: SparkConf = new SparkConf()): SparkContext = {
    SparkContext.getOrCreate(conf)
  }
}


/**
  * 构建SparkSession实例对象工具类
  */
object SparkSessionUtils{

  /**
    * 根据给定的SparkConf创建SparkSession对手
    * @param conf
    *             SparkConf实例对象
    * @param integratedHive
    *                       是否集成HIve，默认值false
    * @return
    */
  def getSparkSession(conf: SparkConf = new SparkConf(),
                      integratedHive: Boolean = false): SparkSession ={
    if(integratedHive){
      SparkSession.builder().config(conf) // 相关应用配置
        // 集成Hive
        .config(TrackConstants.HIVE_METASTORE_URIS, ConfigurationManager.getProperty(TrackConstants.HIVE_METASTORE_URIS))
        .config(TrackConstants.HIVE_METASTORE_WAREHOUSE_DIR, ConfigurationManager.getProperty(TrackConstants.HIVE_METASTORE_WAREHOUSE_DIR))
        .enableHiveSupport()
        .getOrCreate()
    }else{
      SparkSession.builder().config(conf).getOrCreate()
    }
  }
}
