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
  columns.trackingColumns = List()
  columns.otherColumns = List("open_time", "mac", "wifi_mac", "product_model", "product_serial", "promotion_channel",
    "last_login_time")

  sourceColumnMap = Map(
    "open_time" -> "openTime",
    "last_login_time" -> "lastLoginTime"
  )

  sourceFilterWhere = "user_id is not null and user_id <> ''"
  sourceDb = MysqlDB.medusaTvServiceAccount

  dimensionName = "dim_medusa_terminal_user"
}
