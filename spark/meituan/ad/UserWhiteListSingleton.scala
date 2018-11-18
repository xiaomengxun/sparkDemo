package com.erongda.bigdata.spark.meituan.ad

import com.erongda.bigdata.jedis.JedisPoolUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * 从Redis中读取白名单用户数据
  */
object UserWhiteListSingleton {

  @volatile private var instance: Broadcast[List[Int]] = _

  def getInstance(sc: SparkContext): Broadcast[List[Int]] = {
    if (instance == null) {
      // =================================================
      synchronized {
        if (instance == null) {
          // 1. 从Redis中读取数据白名单数据
          val whiteListUsers: List[Int] = {
            Try{
              // 获取 Jedis连接
              val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
              // 从Redis中依据Key读取黑名单用户
              import scala.collection.JavaConverters._
              val whiteSetUsers: mutable.Set[String] = jedis.smembers("ad:user:white").asScala
              // 返回
              (jedis, whiteSetUsers.toList)
            }match {
              case Success((jedisValue, whiteUsers)) =>
                // 关闭连接
                JedisPoolUtil.release(jedisValue)
                // 返回
                whiteUsers.map(_.toInt)
              case Failure(exception) => throw exception
            }
          }

          // 2. 使用SparkContext将白名单数据广播出去
          instance = sc.broadcast(whiteListUsers)
        }
      }
      // =================================================
    }
    instance
  }


}
