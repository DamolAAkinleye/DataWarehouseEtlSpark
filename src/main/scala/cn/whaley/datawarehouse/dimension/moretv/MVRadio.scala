package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 电台维度表
  */
object MVRadio extends DimensionBase {

  dimensionName = "dim_medusa_mv_radio"

  columns.skName = "mv_radio_sk"

  columns.primaryKeys = List("mv_radio_id")

  columns.trackingColumns = List()

  columns.otherColumns = List("mv_radio_title", "mv_radio_create_time", "mv_radio_publish_time")


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_mvradio", "id", 1, 134, 1)

  sourceFilterWhere = "sid is not null and sid <> ''"

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.otherColumns(0) -> "title",
    columns.otherColumns(1) -> "create_time",
    columns.otherColumns(2) -> "publish_time"
  )




}
