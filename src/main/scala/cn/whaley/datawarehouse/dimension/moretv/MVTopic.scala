package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 音乐精选集维度表
  */
object MVTopic extends DimensionBase {

  dimensionName = "dim_medusa_mv_topic"

  columns.skName = "mv_topic_sk"

  columns.primaryKeys = List("mv_topic_sid")

  columns.trackingColumns = List()

  columns.otherColumns = List("mv_topic_name", "mv_topic_create_time", "mv_topic_publish_time")



  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "cid",
    columns.otherColumns(0) -> "title",
    columns.otherColumns(1) -> "cast(create_time as timestamp)",
    columns.otherColumns(2) -> "cast(publish_time as timestamp)"
  )

  sourceDb = MysqlDB.medusaCms("mtv_mvtopic", "id", 1, 134, 1)

  sourceFilterWhere = "account_id is not null and account_id <> ''"






}
