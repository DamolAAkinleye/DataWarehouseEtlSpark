package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}


/**
  * Created by baozhiwang on 2017/4/24.
  * 常用类型
  *('site_tv','site_movie','site_xiqu','site_comic','site_zongyi','site_hot','site_jilu')
  */
object CommonPathParseUtils {
  def commonListCategoryDimension() :DimensionColumn = {
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))), "web_location_sk")
  }
}
