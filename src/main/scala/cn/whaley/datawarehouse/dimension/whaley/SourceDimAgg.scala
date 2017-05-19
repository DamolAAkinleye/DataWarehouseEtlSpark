package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame

/**
  * 创建人：郭浩
  * 创建时间：2017/5/19
  * 程序作用：
  * 数据输入：
  * 数据输出：
  */
object SourceDimAgg extends DimensionBase {
  columns.skName = "source_site_sk"
  columns.primaryKeys = List("source_site_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
  )

  readSourceType = jdbc
  dimensionName = "dim_whaley_source_site"

  override def readSource(readSourceType: Value): DataFrame = {
    val df:DataFrame = null
    /**
      * 站点树维度
      */
    val sourceSite = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_SOURCE_SITE)
                   .filter("dim_invalid_time is null")
                   .selectExpr("'source_site' as source_code","'首页各区域' as source_name",
                               "last_second_code as module_code","last_second_name as module_name","last_first_code as sub_module_code","last_first_name as sub_module_name")
                   .distinct()
    /**
      * 首页入口维度
      */
    val launcherEntrance = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_LAUNCHER_ENTRANCE)
      .filter("dim_invalid_time is null")
      .selectExpr("'launcher_entrance' as source_code","'首页各区域' as source_name",
        "access_area_code as module_code","access_area_name as module_name","access_location_code as sub_module_code","access_location_name as sub_module_name")
      .distinct()

    /**
      * 各频道首页维度
      */
    val channelEntrance = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_PAGE_ENTRANCE)
      .filter("dim_invalid_time is null")
      .selectExpr("'channelEntrance' as source_code","'频道首页各区域' as source_name",
        "access_area_code as module_code","access_area_name as module_name","access_location_code as sub_module_code","access_location_name as sub_module_name")
      .distinct()



    df
  }
}