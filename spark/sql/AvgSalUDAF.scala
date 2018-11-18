package com.erongda.bigdata.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义聚合函数UDAF，求取各个部门的平均工资
  *     SELECT deptno, AVG(sal) AS avg_sal FROM default.emp GROUP BY deptno ;
  */
object AvgSalUDAF extends UserDefinedAggregateFunction{

  /**
    * 表示的是聚合函数输入的参数类型，封装在StructType里面的
    * @return
    */
  override def inputSchema: StructType = StructType(
    Array(StructField("sal", DoubleType, nullable = true))
  )

  /**
    * 表示的是聚合过程中，缓冲临时变量的数据类型，也是封装在StructType中
    * @return
    */
  override def bufferSchema: StructType = StructType(
    StructField("sal_total", DoubleType, nullable = true) ::
    StructField("sal_count", IntegerType, nullable = true) :: Nil
  )

  /**
    * 聚合函数的返回值数据类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 确定唯一性：
    *       Returns true iff this function is deterministic,
    *  i.e. given the same input, always return the same output.
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 表示的是对聚合中的临时缓冲值的初始化
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0)
  }

  /**
    * 分区聚合：
    *     针对每个分区数据进行操作的
    *     表示的是针对每条数据进行聚合函数以后，对聚合缓冲临时变量值的更新
    * @param buffer
    *               聚合缓冲的临时变量
    * @param input
    *              输入变量
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取 缓冲临时变量的值
    val salTotal = buffer.getDouble(0)
    val salCount = buffer.getInt(1)

    // 获取输入传递进来的值
    val inputSal = input.getDouble(0)

    // 更新缓冲数据中的值
    buffer.update(0, salTotal + inputSal)
    buffer.update(1, salCount + 1)
  }

  /**
    * TODO: 针对所有分区的聚合结果进行全局聚合
    *   -a. This is called when we merge two partially aggregated data together.
    *       从字面意思，就是合并各个分区聚合结果到一起
    *   -b. Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    *       表示将两个聚合的缓冲的数据，合并以后并存储更新到buffer1中
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // i. 分别获取两个缓冲临时变量的值
    val salTotal1 = buffer1.getDouble(0)
    val salCount1 = buffer1.getInt(1)

    val salTotal2 = buffer2.getDouble(0)
    val salCount2 = buffer2.getInt(1)

    // 合并并更新
    buffer1.update(0, salTotal1 + salTotal2)
    buffer1.update(1, salCount1 + salCount2)

  }

  /**
    * 表示的是依据 聚合缓冲的临时变量，计算 最终弄 聚合结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    // a. 从缓冲中获取临时变量的值
    val salTotal = buffer.getDouble(0)
    val salCount = buffer.getInt(1)

    // b. 计算并返回
    salTotal / salCount
  }
}
