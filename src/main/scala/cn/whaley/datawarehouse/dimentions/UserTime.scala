package cn.whaley.datawarehouse.dimentions

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.HdfsUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Tony on 16/12/21.
  *
  * 时间维度ETL程序
  *
  * 无需自动触发，只需要修改后手动执行一次
  */
object UserTime extends BaseClass {

  override def execute(args: Array[String]): Unit = {

    val rdd = sc.makeRDD(getTimeSeq.map(s => Row.fromTuple(s)))

    val schema = new StructType(Array(
      StructField("time_key", StringType),
      StructField("hour", IntegerType),
      StructField("minute", IntegerType),
      StructField("second", IntegerType),
      StructField("period", StringType)
    ))

    val df = sqlContext.createDataFrame(rdd, schema)
    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_time")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_time")

  }

  private def getTimeSeq: List[(String, Int, Int, Int, String)] = {
    val result = collection.mutable.Buffer[(String, Int, Int, Int, String)]()
    (0 to 23).foreach(hour => {
      val period = hour match {
        case 0 | 1 | 2 | 3 | 4 | 5 => "凌晨"
        case 6 | 7 | 8 | 9 | 10 | 11 => "上午"
        case 12 | 13 => "中午"
        case 14 | 15 | 16 | 17 => "下午"
        case 18 | 19 | 20 | 21 | 22 | 23 => "晚上"
      }
      (0 to 59).foreach(minute =>
        (0 to 59).foreach(sec => {
          val row = (s"$hour:$minute:$sec", hour, minute, sec, period)
          result += row
        })
      )
    })
    result.toList
  }
}
