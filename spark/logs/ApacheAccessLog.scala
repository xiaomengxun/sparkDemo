package com.erongda.bigdata.spark.logs

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

/**
  * 用于封装解析的日志数据
  *   64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  *
  * @param ipAddress
  * @param clientIdented
  * @param userId
  * @param dateTime
  * @param method
  * @param endpoint
  * @param protocol
  * @param responseCode
  * @param contentSize
  */
case class ApacheAccessLog(
ipAddress: String, clientIdented: String,
userId: String, dateTime: String,
method: String, endpoint: String,
protocol: String, responseCode: Int,
contentSize: Long)

/**
  * 用于解析Apache 访问日志
  */
object ApacheAccessLog {

  // 正则表达式， 一般情况下 以^开头, 以$ 结尾
  // 64.242.88.10 - - [07/Mar/2004:16:47:12 -0800] "GET /robots.txt HTTP/1.1" 200 68
  val PARTTERN: Regex = """^(\S+) (-|\S+) (-|\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)$""".r

  /**
    * 对数据进行过滤，不符合正则表达式的过滤掉，否则后续解析出错
    * @param log
    * @return
    */
  def isValidateLogLine(log: String): Boolean = {
    // 使用正则表达式进行匹配
    val matchIn: Option[Match] = PARTTERN.findFirstMatchIn(log)

    // return
    matchIn.nonEmpty
  }

  /**
    * 用于解析log文件，将每行数据解析对应成CASE CLASS
    * @param log
    * @return
    */
  def parseLogLine(log: String): ApacheAccessLog = {
    // 使用正则表达式进行匹配
    // parse log info
    val matchIn: Option[Match] = PARTTERN.findFirstMatchIn(log)

    if(matchIn.isEmpty){
      throw new RuntimeException(s"Cannot parse log line: ${log}")
    }

    // get value
    val matchValue: Match = matchIn.get

    // return
    ApacheAccessLog(
      matchValue.group(1),
      matchValue.group(2),
      matchValue.group(3),
      matchValue.group(4),
      matchValue.group(5),
      matchValue.group(6),
      matchValue.group(7),
      matchValue.group(8).toInt,
      matchValue.group(9).toLong
    )
  }

}
