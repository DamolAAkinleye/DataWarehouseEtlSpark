package cn.whaley.datawarehouse.temp

import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{DataFrameUtil, HdfsUtil, Params}
import org.apache.commons.lang3.time.DateUtils


/**
  * Created by Tony on 17/3/7.
  */
object IncrementTest extends BaseClass {

  val PRIMARY_KEYS = List("id")
  val TRACKING_COLUMNS = List("title")
  //变化需要记录历史的列
  val OTHER_COLUMNS = List("code", "type")
  //  val COLUMNS = List("id", "code", "title", "type")
  val SK_NAME = "sk"
  val VALID_TIME_KEY = "dim_valid_time"
  val INVALID_TIME_KEY = "dim_invalid_time"


  override def execute(params: Params): Unit = {

    val COLUMNS = PRIMARY_KEYS ++ TRACKING_COLUMNS ++ OTHER_COLUMNS

    val sourcedb = {
      Map("url" -> "jdbc:mysql://10.10.2.16:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "dbtable" -> "test",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "dw_user",
        "password" -> "dw_user@wha1ey")
    }

    val today = DateUtils.ceiling(new Date(), Calendar.DATE)

    //读取源数据
    val sourceDF = sqlContext.read.format("jdbc").options(sourcedb).load()

    //过滤源数据
    val filteredSourceDf = sourceDF.selectExpr(COLUMNS: _*)

    if (!HdfsUtil.pathIsExist("/data_warehouse/dw_dimensions/test")) {

      val result = DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(filteredSourceDf, today, null),
        SK_NAME
      )
    }

    //读取现有维度
    val originalDf = sqlContext.read.parquet("/data_warehouse/dw_dimensions/test")


    val addDf =
    //新增的行
      filteredSourceDf.as("b").join(
        originalDf.where(INVALID_TIME_KEY + " is null").as("a"), PRIMARY_KEYS, "leftouter"
      ).where(
        "a." + SK_NAME + " is null"
      ).selectExpr(
        COLUMNS.map(s => "b." + s): _*
      ).unionAll(
        //追踪列变化的行
        filteredSourceDf.as("b").join(
          originalDf.where(INVALID_TIME_KEY + " is null").as("a"), PRIMARY_KEYS, "leftouter"
        ).where(
          TRACKING_COLUMNS.map(s => "a." + s + " != b." + s).mkString(" or ")
        ).selectExpr(COLUMNS.map(s => "b." + s): _*)
      )


    val df = originalDf.as("a").join(
      filteredSourceDf.as("b"), PRIMARY_KEYS, "leftouter"
    ).selectExpr(
      List("a." + SK_NAME) ++ PRIMARY_KEYS.map(s => "a." + s) ++ TRACKING_COLUMNS.map(s => "a." + s)
        ++ OTHER_COLUMNS.map(s => "b." + s) ++ List(VALID_TIME_KEY, INVALID_TIME_KEY).map(s => "a." + s): _*
    )


    val result = df.unionAll(
      DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(addDf, today, null)
        , SK_NAME
        , df.selectExpr("max(" + SK_NAME + ")").first().getLong(0)
      )
    )

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/test")
    df.write.parquet("/data_warehouse/dw_dimensions/test")
  }

}
