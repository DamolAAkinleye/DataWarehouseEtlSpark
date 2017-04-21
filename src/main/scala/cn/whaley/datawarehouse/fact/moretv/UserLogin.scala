package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.DateFormatUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by Tony on 17/4/5.
  */
object UserLogin extends FactEtlBase {

  topicName = "fact_medusa_user_login"

  parquetPath = LogPath.LOGIN_LOG_PATH

  addColumns = List(
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("ip")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("datetime")),
    UserDefinedColumn("app_series", udf(getAppSeries: String => String), List("version")),
    UserDefinedColumn("app_version", udf(getAppVersion: String => String), List("version"))

  )

  columnsFromSource = List(
    ("product_serial", "ProductSerial"),
    ("sys_ver", "SysVer"),
    ("wifi_mac", "WifiMac"),
    ("app_name", "appName"),
    ("ip", "ip"),
    ("mac", "mac"),
    ("product_model", "productModel"),
    ("product_version", "productVersion"),
    ("promotion_channel", "promotionChannel"),
    ("sn", "sn"),
    ("log_timestamp", "timestamp"),
    ("user_id", "userId"),
    ("user_type", "userType"),
    ("version", "version"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionColumns = List(
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))), "web_location_sk"),
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))), "user_sk"),
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))), "product_model_sk"),
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))), "promotion_sk"),
    new DimensionColumn("dim_app_version",
      List(DimensionJoinCondition(Map("app_series" -> "app_series", "app_version" -> "version"), null, List(("build_time", false)))), "app_version_sk")
  )

  override def readSource(startDate: String): DataFrame = {
    //电视猫的读取目录需要加一天
    val date = DateUtils.addDays(DateFormatUtils.readFormat.parse(startDate), 1)
    super.readSource(DateFormatUtils.readFormat.format(date))
  }

  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

}
