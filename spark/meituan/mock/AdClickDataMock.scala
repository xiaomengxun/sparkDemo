package com.erongda.bigdata.spark.meituan.mock

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 模拟产生广告点击数据：
  * -1. 创建Topic
      bin/kafka-topics.sh --create --zookeeper bigdata-training01.erongda.com:2181/kafka --replication-factor 2 --partitions 3 --topic adTopic
    -2. 通过Console模拟生产者，向Topic中发送数据
      bin/kafka-console-producer.sh --broker-list bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094 --topic adTopic
    -3. 从Topic中消费数据
      bin/kafka-console-consumer.sh --zookeeper bigdata-training01.erongda.com:2181/kafka --topic adTopic --from-beginning
  */
object AdClickDataMock {
  // 分割字符串
  val delimiter = " "
  val topicName = "adTopic"


  def main(args: Array[String]): Unit = {
    // 并发环境原子性操作
    val running = new AtomicBoolean(true)

    // Kafka Cluster相关信息
    val brokerList = "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094"
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    // 构建生产者Producer实例对象
    val config: ProducerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // 启动两个线程，并发产生数据发送至Topic中
    for (i <- 0 until 4) {
      new Thread(new Runnable {
        override def run(): Unit = {
          // 数据随机器
          val random = Random
          while (running.get()) {
            // 1. 随机数据
            val messages = generateMessage(random)
            // 2. 发送
            for (message <- messages) {
              producer.send(message)
            }
            // 3. 休息一下
            Thread.sleep(random.nextInt(5) + 1)
          }
        }
      }).start()
    }

    // 运行2min后关闭
    Thread.sleep(3 * 60 * 60 * 1000)
    running.set(false)
    producer.close
  }

  /**
    * 产生一个随机的数据
    * @return
    */
  def generateMessage(random: Random): List[KeyedMessage[String, String]] = {
    val key = random.nextInt(100).toString
    // 0-999
    val cityId = random.nextInt(300) + 1
    val province = s"province_${cityId % 34 + 1}"
    val city = s"city_$cityId"
    val userId = random.nextInt(100000)
    val adId = random.nextInt(100)
    val str = s"$province$delimiter$city$delimiter$userId$delimiter$adId"

    // 0.05的几率产生一次产生多条数据, num >= 1
    val numbers = if (random.nextDouble() <= 0.05) {
      random.nextInt(500) + 1
    } else {
      1
    }

    val msgs = (0 until numbers).foldLeft(ArrayBuffer[KeyedMessage[String, String]]())((buf, b) => {
      val timestamp = System.currentTimeMillis()
      val value = s"$timestamp$delimiter$str"
      val msg = KeyedMessage[String, String](topicName, key, key, value)

      buf += msg
      buf
    })

    //返回对象
    msgs.toList
  }
}
