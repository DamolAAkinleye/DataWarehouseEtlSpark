package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端电台维度表
  */
object Radio extends DimensionBase {
  columns.skName = "radio_sk"
  columns.primaryKeys = List("radio_code")
  columns.trackingColumns = List()
  columns.otherColumns = List("radio_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(

  )

  sourceFilterWhere = "radio_code is not null and radio_code <> ''"
  sourceDb = MysqlDB.medusaUCenterMember

  dimensionName = "dim_whaley_radio"
}
