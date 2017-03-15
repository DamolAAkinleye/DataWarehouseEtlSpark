package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 3/14/17.
  * 直播节目维度表
  */
object LiveProgram extends DimensionBase {

  dimensionName = "dim_medusa_live_program"

  columns.skName = "live_program_sk"

  columns.primaryKeys = List("live_program_sid")

  columns.trackingColumns = List()

  columns.otherColumns = List(
    "live_program_title", "live_program_source", "live_program_site", "live_program_create_time",
    "live_program_publish_time"
  )


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_live_program", "id", 1, 250, 1)

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._

    sourceDf.filter($"sid".isNotNull).dropDuplicates("sid" :: Nil)
      .select(
        $"sid".as(columns.primaryKeys(0)),
        $"title".as(columns.otherColumns(0)),
        $"source".as(columns.otherColumns(1)),
        $"site".as(columns.otherColumns(2)),
        $"create_time".cast("timestamp").as(columns.otherColumns(3)),
        $"publish_time".cast("timestamp").as(columns.otherColumns(4))
      )
  }


}