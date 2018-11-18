package com.erongda.bigdata.spark.meituan.dao

import java.sql.{Connection, DriverManager}

import com.erongda.bigdata.spark.meituan.util.{ConfigurationManager, TrackConstants}

/**
  * 操作数据库DAO层，获取Connection连接
  */
object JDBCHelper {

  /**
    * 获取MySQL数据库Connection连接，为了简化期间不适用连接池
    *
    * @return
    */
  def getMySQLConnection: Connection = {
    // 获取conncetion
    Class.forName(ConfigurationManager.getProperty(TrackConstants.JDBC_DRIVER))
    // 连接数据库三要素
    val url = ConfigurationManager.getProperty(TrackConstants.JDBC_URL)
    val username = ConfigurationManager.getProperty(TrackConstants.JDBC_USER)
    val password = ConfigurationManager.getProperty(TrackConstants.JDBC_PASSWORD)
    // 获取连接并返回
    DriverManager.getConnection(url, username, password)
  }

}
