package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.whaley.Radio.sourceColumnMap
import cn.whaley.datawarehouse.dimension.whaley.Subject.sourceFilterWhere
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by czw on 17/3/10.
  *
  * 微鲸端节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.otherColumns = List("title", "content_type", "duration", "area", "video_type", "publish_time", "douban_Id", "source", "parent_sid",
     "year", "language_code", "video_length_type", "create_time","supply_type","tags")

  sourceColumnMap = Map(
    "content_type" -> "contentType",
    "video_type" -> "videoType",
    "publish_time" -> "publishTime",
    "create_time" -> "createTime",
    "video_length_type" -> "videoLengthType",
    "parent_sid" -> "parentId",
    "douban_Id" -> "doubanId"
  )

  sourceFilterWhere = "sid is not null and sid <> ''"
  sourceDb = MysqlDB.whaleyTvService("mtv_program", "sid", 1, 2010000000, 500)

  dimensionName = "dim_whaley_program"

}
