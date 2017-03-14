package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 3/13/17.
  * 音乐精选集维度表
  */
object MVTopic extends DimensionBase {

  dimensionName = "dim_medusa_mv_topic"

  columns.skName = "mv_topic_sk"

  columns.primaryKeys = List("mv_topic_sid")

  columns.trackingColumns = List()

  columns.otherColumns = List(
    "mv_topic_name",
    "mv_topic_create_time",
    "mv_topic_publish_time"
  )


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_mvtopic", "id", 1, 134, 1)

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._

    sourceDf.dropDuplicates("sid" :: Nil)
      .select(
        $"sid".as(columns.primaryKeys(0)),
        $"title".as(columns.otherColumns(0)),
        $"create_time".cast("timestamp").as(columns.otherColumns(1)),
        $"publish_time".cast("timestamp").as(columns.otherColumns(2))
      )
  }
}
