package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Tony on 17/3/8.
  *
  * 电视猫账号维度表
  */
object Account extends DimensionBase {
  columns.skName = "account_sk"
  columns.primaryKeys = List("account_id")
  columns.trackingColumns = List()
  columns.otherColumns = List("user_name", "email", "mobile", "reg_time", "register_from")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "account_id" -> "moretvid",
    "user_name" -> "username",
    "reg_time" -> "cast(regdate as timestamp)",
    "register_from" -> "registerfrom"
  )

  sourceFilterWhere = "account_id is not null and account_id <> ''"
  sourceDb = MysqlDB.medusaUCenterMember

  dimensionName = "dim_medusa_account"
}
