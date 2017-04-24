package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫用户维度表
  */
object TerminalUser extends DimensionBase {

  columns.skName = "user_sk"
  columns.primaryKeys = List("user_id")
  columns.trackingColumns = List("apk_version", "ip")
  columns.allColumns = List(
    "user_id",
    "open_time",
    "ip",
    "mac",
    "wifi_mac",
    "product_model",
    "product_serial",
    "promotion_channel",
    "system_version",
    "user_type",
    "apk_version",
    "current_ip",
    "current_apk_version"
  )

  sourceColumnMap = Map(
    "open_time" -> "openTime",
    "user_type" -> "userType",
    "apk_current_version" -> "current_version",
    "apk_version" -> "current_version",
    "current_ip" -> "ip"
  )

  sourceFilterWhere = "user_id is not null and user_id <> ''"
  sourceDb = MysqlDB.medusaTvServiceAccount

  dimensionName = "dim_medusa_terminal_user"
}
