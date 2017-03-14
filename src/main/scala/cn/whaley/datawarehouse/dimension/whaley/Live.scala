package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端直播维度表
  */
object Live extends DimensionBase {
  columns.skName = "live_sk"
  columns.primaryKeys = List("live_code")
  columns.trackingColumns = List()
  columns.otherColumns = List("live_name","content_type")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(

  )

  sourceFilterWhere = "live_code is not null and live_code <> ''"
  sourceDb = MysqlDB.medusaUCenterMember

  dimensionName = "dim_whaley_live"
}
