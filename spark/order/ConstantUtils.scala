package com.erongda.bigdata.spark.order

/**
  * 存储一些常量数据
  */
object ConstantUtils {

  /**
    * 定义KAFKA相关集群配置信息
    */
  // KAFKA CLUSTER BROKERS
  val METADATA_BROKER_LIST = "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094"

  // OFFSET
  val AUTO_OFFSET_RESET = "largest"

  // 序列化类
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"
  val NEW_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"

  // 发送数据方式
  val PRODUCER_TYPE = "async"

  // Topic的名称
  val ORDER_TOPIC = "orderTopic"


  // 针对 Kakfa来说 New Producer API 配置属性的名称
  val BOOTSTRAP_SERVERS_NAME = "bootstrap.servers"
  val KEY_SERIALIZER_NAME = "key.serializer"
  val VALUE_SERIALIZER_NAME = "value.serializer"

  val SEND_KAFKA_TOPIC_NAME = "order"

}
