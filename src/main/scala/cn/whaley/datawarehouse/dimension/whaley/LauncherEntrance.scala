package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
 * Created by 郭浩 on 17/3/29.
 * 首页入口维度表
 */
object LauncherEntrance extends DimensionBase{

  columns.skName = "launcher_entrance_sk"
  columns.primaryKeys = List("launcher_entrance_id")
  columns.trackingColumns = List()
  columns.allColumns = List("launcher_entrance_id","access_area","access_area_name","access_location","access_location_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "launcher_entrance_id" ->"id"
  )

  sourceFilterWhere = null
  sourceDb = MysqlDB.dwDimensionDb("whaley_launcher_entrance")

  dimensionName = "dim_whaley_launcher_entrance"

}
