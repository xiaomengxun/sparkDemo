package com.erongda.bigdata.spark.risk.hbase

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

/**
  * 操作HBase数据中的表，主要插入数据到表中
  *
  * 创建表的语句：
  *       > create 'testtopic0', { NAME => 'info', COMPRESSION => 'SNAPPY'}, SPLITS => ['b4a8c382-31d9-4d6c-9136-e431953638e1', 'a7b62f88-c07e-434c-a97c-8a0bbf257051', '41ce92b0-a016-4cd0-990c-cdce94bf0e49', '670e99a9-3ba2-406e-8516-6cda3c1c5f8f']
  */
object HBaseDao {

  // 记录日志信息
  @transient lazy val logger = LogManager.getLogger(HBaseDao.getClass)


  // 初始化连接HBase Connection
  private val connection: Connection = createHBaseConn()

  /**
    * 获取连接HBase数据库的Connection对象
    * @return
    */
  def createHBaseConn(): Connection ={
    // 获取配置信息
    val conf = HBaseConfiguration.create()

    // 返回Connection实例对象
    ConnectionFactory.createConnection(conf)
  }

  /**
    * 依据表的名称获取HBase数据库中表的操作句柄
    * @param tableName
    *                  表的名称
    * @return
    */
  def getHTable(tableName: String): HTable = {
    // 获取Table
    val table = connection.getTable(TableName.valueOf(tableName))
    //
    table.asInstanceOf[HTable]
  }
  /**
    * 插入数据到HBase表中
    *
    * @param tableName
    *                  存储到HBase数据库中的表的名称，对应到Topic的名称
    * @param array
    *              对应于RDD中某个分区的数据（Topic中某个分区的数据），里面的数据格式为JSON的字符串
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
  def insert(tableName: String, array: Iterator[(String, String)] ): Boolean ={

    var htable: HTable = null

    try{
      // 获取HBase数据库汇总表的句柄（HTable）
      htable = getHTable(tableName)

      // 创建列表存储Put对象，后续进行批量插入HBase表中
      import java.util
      val puts: util.ArrayList[Put] = new util.ArrayList[Put]()

      // TODO: 解析获取JSON格式的数据，使用Alibaba FastJson库解析数据
      array.foreach{ case(_, message) =>
        // 获取JSONObject对象
        val jsonObj: JSONObject = JSON.parseObject(message)

        // RowKey和Family
        val rowKey: Array[Byte] = Bytes.toBytes(jsonObj.getString("r"))
        val family: Array[Byte] = Bytes.toBytes(jsonObj.getString("f"))

        // Columns 所有列名称和对应的值
        val columns: JSONArray = jsonObj.getJSONArray("q")
        val values: JSONArray = jsonObj.getJSONArray("v")

        // 创建Put对象
        val put = new Put(rowKey)
        // 由于ETL操作，数据量很大，实时进行写入HBase表中，为了提高性，跳过WAL
        put.setDurability(Durability.SYNC_WAL)

        // 加入column
        for(i <- 0 until columns.size()){
          put.addColumn(family, Bytes.toBytes(columns.getString(i)), Bytes.toBytes(values.getString(i)))
        }

        // 加入到list中
        puts.add(put)
      }


      // Unit put(puts: util.List[Put])
      htable.put(puts)

      // 返回值
      true
    }catch {
      case e: Exception => e.printStackTrace()
        logger.warn(s"================== insert $tableName error", e)
        // 返回
        false
    }finally {
      if(null != htable) htable.close()
    }
  }

}
