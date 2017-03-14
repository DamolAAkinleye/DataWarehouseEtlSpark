package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/14/17.
  * 直播节目维度表
  */
object LiveProgram extends DimensionBase {

  dimensionName = "dim_medusa_live_program"

  columns.skName = "live_program_sk"

  columns.primaryKeys = List("live_program_sid")

  columns.trackingColumns = List()

  columns.otherColumns = List(
    "live_program_title", "live_program_source", "live_program_site", "live_program_create_time",
    "live_program_publish_time"
  )


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_live_program", "id", 1, 250, 1)

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.otherColumns(0) -> "title",
    columns.otherColumns(1) -> "source",
    columns.otherColumns(2) -> "site",
    columns.otherColumns(3) -> "create_time",
    columns.otherColumns(4) -> "publish_time"
  )

  sourceFilterWhere = "sid is not null and sid <> ''"



}
