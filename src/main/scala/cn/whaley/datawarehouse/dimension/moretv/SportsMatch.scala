package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by witnes on 3/13/17.
  * 体育比赛维度表
  */
object SportsMatch extends DimensionBase {


  dimensionName = "dim_medusa_sports_match"

  columns.skName = "match_sk"

  columns.primaryKeys = List("match_sid")

  columns.otherColumns = List(
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
    columns.otherColumns(0) -> "title",
    columns.otherColumns(1) -> "sub_title",
    columns.otherColumns(2) -> "category",
    columns.otherColumns(3) -> "match_date",
    columns.otherColumns(4) -> "source",
    columns.otherColumns(5) -> "league_id"
  )


  sourceDb = MysqlDB.medusaCms("sailfish_sport_match", "id", 1, 7000, 10)

  sourceFilterWhere = "match_sid is not null and match_sid <> ''"




}
