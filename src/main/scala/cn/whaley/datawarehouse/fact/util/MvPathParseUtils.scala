package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}


/**
  * Created by baozhiwang on 2017/4/24.
  */
object MvPathParseUtils {
  def mvListCategoryDimension() :DimensionColumn = {
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))), "web_location_sk")
  }
}
