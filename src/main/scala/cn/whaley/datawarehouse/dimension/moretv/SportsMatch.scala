package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by witnes on 3/13/17.
  * 体育比赛维度表
  */
object SportsMatch extends DimensionBase {


  dimensionName = "dim_medusa_sports_match"

  columns.skName = "match_sk"

  columns.primaryKeys = List("match_sid")

  columns.allColumns = List(
    "match_sid",
    "match_title",
    "match_sub_title",
    "match_category",
    "match_date",
    "match_source",
    "league_id"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.allColumns(1) -> "title",
    columns.allColumns(2) -> "sub_title",
    columns.allColumns(3) -> "category",
    columns.allColumns(4) -> "match_date",
    columns.allColumns(5) -> "source",
    columns.allColumns(6) -> "league_id"
  )


  sourceDb = MysqlDB.medusaCms("sailfish_sport_match", "id", 1, 7000, 10)

  sourceFilterWhere = "match_sid is not null and match_sid <> ''"




}
