package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.otherColumns = List("title", "content_type", "content_type_name", "duration", "video_type", "episode_index", "parent_sid",
    "area", "year", "video_length_type", "create_time", "publish_time")

  sourceDb = MysqlDB.medusaCms("mtv_basecontent", "id", 1, 2010000000, 500)



  dimensionName = "dim_medusa_program"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
//    sourceDf.persist()

    sourceDf.registerTempTable("mtv_basecontent")

    sqlContext.sql("select sid, first(id) id, first(display_name) display_name, first(content_type) content_type, " +
      " first(duration) duration, first(parent_id) parent_id, first(video_type) video_type," +
      " first(episode) episode, first(area) area, first(year) year, " +
      " first(videoLengthType) videoLengthType, first(create_time) create_time, first(publish_time) publish_time " +
      " from mtv_basecontent where sid is not null and sid <> '' and display_name is not null " +
      " group by sid ").registerTempTable("program_table")

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().registerTempTable("content_type")

    sqlContext.sql("SELECT a.sid, a.display_name as title, " +
      "a.content_type, c.name as content_type_name, a.duration, a.video_type, a.episode as episode_index, " +
      "b.sid as parent_sid, a.area, a.year, a.videoLengthType as video_length_type, " +
      "a.create_time, " +
      "a.publish_time " +
      " from program_table a left join program_table b on a.parent_id = b.id " +
      " left join content_type c on a.content_type = c.code " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id")
  }
}
