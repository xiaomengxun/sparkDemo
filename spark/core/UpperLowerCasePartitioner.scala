package com.erongda.bigdata.spark.core

import org.apache.spark.Partitioner

/**
  * 自定义分区器，实现RDD分区，在进行Shuffle过程中
  */
class UpperLowerCasePartitioner extends Partitioner{

  /**
    * 表示RDD的分区个数
    * @return
    */
  override def numPartitions: Int = 3

  /**
    * 如何进行分区
    * @param key
    *            按照Key进行分区的
    * @return
    *         返回值是{ 0, ..., numParitions -1 }
    */
  override def getPartition(key: Any): Int = {

    // 获取Key第一个字符，转换为整形
    val firstValue = key.asInstanceOf[String].charAt(0).toInt

    if(97 <= firstValue && firstValue <= 122){
      // 第一个分区
      0
    }else if(65 <= firstValue && firstValue <= 90){
      // 第二个分区
      1
    }else{
      // 第三个分区
      2
    }

  }
}
