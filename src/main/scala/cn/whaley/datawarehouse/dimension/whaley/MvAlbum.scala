package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端音乐专辑维度表
  */
object MvAlbum extends DimensionBase {
  columns.skName = "mv_album_sk"
  columns.primaryKeys = List("mv_album_sid")
  columns.trackingColumns = List()
  columns.otherColumns = List("mv_album_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(

  )

  sourceFilterWhere = "mv_album_sid is not null and mv_album_sid <> ''"
  sourceDb = MysqlDB.medusaUCenterMember

  dimensionName = "dim_whaley_mv_album"
}
