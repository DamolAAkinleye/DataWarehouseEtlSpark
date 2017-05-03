package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * 创建人：郭浩
  * 创建时间：2017/5/03
  * 程序作用：开关机日子
  * 数据输入：on/off日志
  * 数据输出：开关机事实表
  */
object OnOff extends FactEtlBase{
  topicName = "fact_whaley_on_off"

  addColumns = List(
  )

  columnsFromSource = List(
    ("current_vip_level", "current_vip_level"),
    ("firmware_version", "firmware_version"),
    ("is_yunos","is_yunos"),
    ("product_line","product_line"),
    ("event","event"),
    ("duration","duration"),
    ("start_time","cast(start_time as long ) "),
    ("end_time","cast(end_time as long ) "),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")
  )
  dimensionColumns = List(
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("product_sn" -> "product_sn"))), "product_sn_sk"),
    new DimensionColumn("dim_whaley_account",
    List(DimensionJoinCondition(Map("account_id" -> "account_id"))), "account_sk")
  )

  override def readSource(startDate: String): DataFrame = {

    //ota19 off 新增startTime,endTime
    val flag = DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_OFF,startDate)
      .schema.fieldNames.contains("startTime")
    var offDf = DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_OFF,startDate)
    if(!flag){
      offDf = DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_OFF,startDate).withColumn("startTime",expr("'0'"))
        .withColumn("endTime",expr("'0'"))
    }
    //on,off日志合并
    DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_ON,startDate)
          .selectExpr(
            "productSN as product_sn",
            "productLine as product_line",
            "accountId as account_id",
            "currentVipLevel as current_vip_level",
            "firmwareVersion as firmware_version",
            "isYunos as is_yunos",
            "event",
            "0 as duration",
            "0 as start_time",
            "0 as end_time",
            "substr(datetime,1,10) as dim_date",
            "substr(datetime,12,8) as dim_time"
          ).unionAll(
            offDf
              .selectExpr(
                "productSN as product_sn",
                "productLine as product_line",
                "accountId as account_id",
                "currentVipLevel as current_vip_level",
                "firmwareVersion as firmware_version",
                "isYunos as is_yunos",
                "event",
                "case when duration is null then 0 else duration end  duration",
                "case when startTime is null then 0 else startTime end start_time",
                "case when endTime is null then 0 else endTime end end_time",
                "substr(datetime,1,10) as dim_date",
                "substr(datetime,12,8) as dim_time"
              )
    )

  }

}
