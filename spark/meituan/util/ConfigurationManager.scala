package com.erongda.bigdata.spark.meituan.util

import java.io.InputStream
import java.util.Properties

/**
  * Read Configuration Information
  */
object ConfigurationManager {

  // create Properties
  private val props: Properties = new Properties()

  // load configuration properties file
  try{
    // create InputStream
    val inStream: InputStream = ConfigurationManager.getClass.getClassLoader
      .getResourceAsStream("usertrack.properties")
    // load properties
    props.load(inStream)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
    * get Boolean value By key
    *
    * @param key
    * @return value
    */
  def getBoolean(key: String): Boolean = {
    var value: Boolean = false
    try {
      value = getProperty(key).toBoolean
    }catch {
      case e: Exception => e.printStackTrace()
    }
    value
  }

  /**
    * get value By key
    *
    * @param key
    * @return
    */
  def getProperty(key: String): String = props.getProperty(key)

}
