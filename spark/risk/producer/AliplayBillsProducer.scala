package com.erongda.bigdata.spark.risk.producer

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, Properties, Random, UUID}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.codehaus.jettison.json.{JSONArray, JSONObject}


/**
  * 模拟生成 支付宝 账单详情数据
  *   （实际通过爬虫获取数据，组合JSON格式，调用Dubbo服务发送到Topic中）
  * 数据相关说明：aliplay_bills
  *   RowKey: 省份证ID_数据维度类型_订单时间
  *     如：000001_alipay_201708030427
  *   ColumnFamily：info
  *   Columns:
  *     customerId: 用户ID，针对支付宝来说就是支付宝账号ID, 暂定手机号码或者邮箱
  *     orderId: 订单ID，依据日期时间+十七位数字（依据不同的类型等规则生成的） = 25 位的数字
  *     tradeAmount: 交易金额
  *     goodsDesc: 商品说明
  *     payment: 付款方式： 余额宝\支付宝\信用卡\花呗\银行卡
  *     tradeStatus: 交易状态：成功\失败
  *     receiptAmount: 收款账号
  *  TODO:
  *     针对支付宝 账单数据，不同消费，账单的数据 字段不一样，所以此处使用HBase存储最为合适不过
  */
object AliplayBillsProducer {

  def main(args: Array[String]): Unit = {
    // 每隔多长时间发送一次数据
    val schedulerInterval = 1  // 秒
    // 每次发送数据条目数据
    val sendCount: Int = 5000 // 千条

    // 1. 创建一个生产者对象
    // 1.1 读取配置文件
    val prop = new Properties()
    prop.load( getClass.getClassLoader.getResourceAsStream("producer.properties"))

    // 1.2 创建ProducerConfig
    val producerConfig = new ProducerConfig( prop )
    // 1.3 创建Producer实例，生成数据
    val producer = new Producer[String, String](producerConfig)

    // 2. 构造message
    val topic = "testtopic0"

    // 支付方式
    val paymentList = List("余额宝","支付宝","信用卡","花呗","银行卡")
    // 交易状态
    val tradeStatusList = List("成功", "失败")

    val random = new Random()
    /**
      * Json格式：
      *     {
      *         "r": "00000-0",
      *         "f": "d",
      *         "q": [
      *             "customerId"
      *          ],
      *         "v": [
      *                 "0"
      *          ],
      *         "t": "1494558616338"
      *     }
      */
    val list = new scala.collection.mutable.ListBuffer[KeyedMessage[String, String]]()
    while (true){
      val startTime = System.currentTimeMillis()
      // 清空
      list.clear()

      for(index <- 1 to sendCount){
        // prepare bill data
        val bill = new JSONObject()

        // 此处使用UUID代替，实际需要依据RowKey进行查询数据的
        val rowKey = UUID.randomUUID().toString
        // 将Scala集合转换为Java集合 - 隐式转换
        import scala.collection.JavaConversions._
        // 列名
        val columns: JSONArray = new JSONArray(
          List("orderId", "tradeAmount", "goodsDesc", "payment", "tradeStatus", "receiptAmount")
        )
        // 每一列对应的值
        val values: JSONArray = new JSONArray(
          List(
            getOrderId(random), getTradeAmount(20, random.nextInt(500) + 1),
            getGoodsDesc(random, random.nextInt(30)), paymentList(random.nextInt(5)),
            tradeStatusList(random.nextInt(2)), getGoodsDesc(random, random.nextInt(10))
          )
        )

        bill
          .put("r", rowKey) // add RowKey
          .put("f", "info")
          .put("q", columns)
          .put("v", values)

        val message = new KeyedMessage[String, String](topic, rowKey, bill.toString)
        list += message
      }

      // 3. 批量发送  def send(messages: KeyedMessage[K,V]*)
      producer.send(list.toList: _*)

      val endTime = System.currentTimeMillis()
      println(s"send messages: ${list.length}, spent time : ${endTime - startTime}")

      // 线程暂停
      Thread.sleep(1000 * schedulerInterval)
    }
  }


  /**
    *   用户ID，针对支付宝来说就是支付宝账号ID, 暂定手机号码或者邮箱
    */
  def getCustomerId(random: Random): String = {
    val sb = new StringBuffer("1")
    for(index <- 1 to 10){
      sb.append(random.nextInt(10))
    }
    // 返回
    sb.toString
  }

  /**
    * orderId: 订单ID，依据日期时间+十七位数字（依据不同的类型等规则生成的） = 25 位的数字
    * @param random
    * @return
    */
  def getOrderId(random: Random): String = {
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date())
    val sb = new StringBuffer(date)
    for(index <- 1 to 17){
      sb.append(random.nextInt(10))
    }
    // 返回
    sb.toString
  }

  /**
    * tradeAmount: 交易金额
    */
  def getTradeAmount(start: Int, end: Int): String = {
    // 随机生成某个范围内的Double类型数据
    val price: Double = start + Math.random() * end % (end - start + 1)
    // 保留两位小数
    new DecimalFormat("#.00").format(price)
  }

  /**
    * goodsDesc: 商品说明
    *   此处使用随机生成字符串代替
    */
  def getGoodsDesc(random: Random, size: Int): String = {
    val str = "abcdefghijklmnopqrstuvwxyz"
    val len = str.length()

    val sb = new StringBuffer()
    for (i <- 0 to (len + size)) {
      sb.append(str.charAt(random.nextInt(len - 1)))
    }

    sb.toString
  }
}
