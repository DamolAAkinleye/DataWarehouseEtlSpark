package cn.whaley.datawarehouse.temp

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by Tony on 17/3/8.
  */
object IncrementNewTest extends DimensionBase{
  columns.primaryKeys = List("code")
  columns.trackingColumns = List("title")
  columns.allColumns = List("code","title","type")
  columns.skName = "sk"

  sourceDb = MysqlDB.dwDimensionDb("test")

  dimensionName = "test"
}
