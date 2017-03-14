package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 直播频道维度表
  */
object Channel extends DimensionBase {

  dimensionName = "dim_medusa_channel"

  columns.skName = "channel_sk"

  columns.primaryKeys = List("channel_code")

  columns.otherColumns = List(
    "channel_name", "channel_source_code", "channel_source_name", "channel_create_time", "channel_publish_time"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.otherColumns(1) -> "station",
    columns.otherColumns(2) -> "station_code",
    columns.otherColumns(3) -> "source",
    columns.otherColumns(4) -> "cast(create_time as timestamp)",
    columns.otherColumns(5) -> "cast(publish_time as timestamp)"
  )

  sourceDb = MysqlDB.medusaCms("mtv_channel", "id", 1, 134, 1)

  sourceFilterWhere = "channel_code is not null and channel_code <> ''"


}
