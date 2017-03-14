package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸专题维度表
  */
object Subject extends DimensionBase {
  columns.skName = "subject_sk"
  columns.primaryKeys = List("subject_code")
  columns.trackingColumns = List()
  columns.otherColumns = List("subject_name", "content_type", "content_type_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "account_id" -> "moretvid",
    "user_name" -> "username",
    "reg_time" -> "cast(regdate as timestamp)",
    "register_from" -> "registerfrom"
  )

  sourceFilterWhere = "subject_code is not null and subject_code <> ''"
  sourceDb = MysqlDB.medusaUCenterMember

  dimensionName = "dim_whaley_subject"
}
