package com.erongda.bigdata.spark.etl

import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  * 订单实体类（Case Class）
  *
  * @param orderType
  *                  不同的订单类型
  * @param orderId
  *                订单ID
  * @param provinceId
  * 省份ID
  * @param orderPrice
  * 订单金额
  */
case class Order(orderType: String, orderId: String, provinceId: Int, orderPrice: Double)

/**
  * 模拟生产订单数据，发送到Kafka Topic中
  *     Topic中每条数据Message类型为String，以JSON格式数据发送
  * 数据转换：
  *     将Order类实例对象转换为JSON格式字符串数据（可以使用fastjson类库）
  *     https://github.com/alibaba/fastjson
  *
  *
bin/kafka-topics.sh --create --zookeeper bigdata-training01.erongda.com:2181/kafka --replication-factor 2 --partitions 3 --topic orders
    -2. 通过Console模拟生产者，向Topic中发送数据
bin/kafka-console-producer.sh --broker-list bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094 --topic orders
    -3. 从Topic中消费数据
bin/kafka-console-consumer.sh --zookeeper bigdata-training01.erongda.com:2181/kafka --topic orders --from-beginning


  */
object OrderProducer {

  def main(args: Array[String]): Unit = {

    /**
      * Jaskson ObjectMapper类将对象转换为JSON格式数据（相互转换）
      */
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    /**
      * 构建一个Kafka Producer 生产者实例对象，以便向Topic发送数据
      */
    val props: Properties = new Properties()
    // Kafak Cluster集群地址
    props.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST)
    // 设置数据序列化（编码方式）
    props.put("serializer.class", ConstantUtils.SERIALIZER_CLASS)
    props.put("key.serializer.class", ConstantUtils.SERIALIZER_CLASS)
    // 设置发送数据为异步发送，此时数据发送很多
    props.put("producer.type", ConstantUtils.PRODUCER_TYPE)

    // 订单的类型
    val orderTypeList = List("alipay", "weixin", "card", "other")

    // 生产者Producer实例对象
    var producer: Producer[String, String] = null
    try {
      producer = new Producer[String, String](new ProducerConfig(props))

      // TODO: 模拟的时候，每次想Topic发送多条数据，需要列表存储
      val messagesArrayBuffer = new ArrayBuffer[KeyedMessage[String, String]]()

      while (true) {
        // 清空
        messagesArrayBuffer.clear()

        // 每次循环 模拟产生的订单数目
        val randomNumber = RandomUtils.getRandomNum(3) + 1
        // TODO: startTime
        val startTime = System.currentTimeMillis()
        // TODO: 使用FOR循环产生订单数据
        for (index <- 0 to randomNumber) {
          // 创建订单实例对象
          val orderId = UUID.randomUUID().toString  // 订单ID
          val provinceId = RandomUtils.getRandomNum(34) + 1  // 省份ID
          val orderPrice = RandomUtils.getRandomNum(50) + 35.50 // 订单金额
          val orderType = orderTypeList(RandomUtils.getRandomNum(4))
          val order = Order(orderType, orderId, provinceId, orderPrice)

          // 构建KeyedMessage
          val message = new KeyedMessage[String, String](
            ConstantUtils.ORDER_TOPIC, //
            orderId, //
            mapper.writeValueAsString(order) //
          )
          // 如何解析JSON格式数据
          // println(mapper.readValue("{\"orderId\":\"713aa2ec-ca1c-47cc-8be3-e214fb5bf1fc\",\"provinceId\":33,\"orderPrice\":73.5}", classOf[Order]))

          // 添加到数组中
          messagesArrayBuffer += message
        }

        // TODO: def send(messages: KeyedMessage[K,V]*), 此处批量将数据发送到Topic
        producer.send(messagesArrayBuffer: _*)
        // TODO: endTime
        val endTime = System.currentTimeMillis()
        println(s"--------------- Send Messages: $randomNumber, Spent Time: ${endTime - startTime}")

        // 线程休眠
        Thread.sleep(RandomUtils.getRandomNum(10) * 10 + 500)
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != producer) producer.close()
    }
  }

}
