package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 推荐应用维度表
  */
object Application extends DimensionBase {

  dimensionName = "dim_medusa_application"

  columns.skName = "application_sk"

  columns.primaryKeys = List("application_sid")

  columns.trackingColumns = List()

  columns.otherColumns = List("application_name", "application_version", "application_version_name")


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.otherColumns(0) -> "title",
    columns.otherColumns(1) -> "version",
    columns.otherColumns(2) -> "version_name"
  )

  sourceDb = MysqlDB.medusaCms("mtv_application", "id", 1, 134, 1)

  sourceFilterWhere = "sid is not null and sid <> ''"




}
