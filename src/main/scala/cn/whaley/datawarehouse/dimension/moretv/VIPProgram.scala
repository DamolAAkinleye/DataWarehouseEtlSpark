package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by xia.jun on 2017/11/9.
  */
object VIPProgram extends DimensionBase{

  columns.skName = "program_sk"
  columns.primaryKeys = List("program_code")
  columns.trackingColumns = List()
  columns.allColumns = List("program_code", "type", "title", "cast", "area", "year", "episode_count","status",
    "video_type", "content_type", "parent_id", "is_auto_bound", "valid_time", "invalid_time", "create_time")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "program_code" -> "program_code",
    "content_id" -> "content_id",
    "type" -> "type",
    "title" -> "title",
    "cast" -> "cast",
    "area" -> "area",
    "year" -> "year",
    "episode_count" -> "episode_count",
    "status" -> "status",
    "video_type" -> "video_type",
    "content_type" -> "content_type",
    "parent_id" -> "parent_id",
    "is_auto_bound"->"is_auto_bound",
    "valid_time"->"valid_time",
    "invalid_time" -> "invalid_time",
    "create_time" -> "create_time"
  )

  sourceFilterWhere = "program_code is not null and program_code <> ''"

  sourceDb = MysqlDB.medusaMemberDB("content_program")



  dimensionName = "dim_medusa_vip_program"


}
