package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by czw on 17/3/10.
  *
  * 微鲸节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.allColumns = List("sid","title", "content_type", "content_type_name", "duration", "video_type", "episode_index",
    "area", "year", "video_length_type", "create_time", "publish_time", "douban_id", "source", "language_code","supply_type","tags")

  sourceDb = MysqlDB.whaleyCms("mtv_basecontent", "id", 1, 2010000000, 500)

  dimensionName = "dim_whaley_program"

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    sourceDf.registerTempTable("mtv_basecontent")

    sqlContext.sql("select sid, first(id) id, first(display_name) display_name, first(content_type) content_type, first(duration) duration, " +
      "first(video_type) video_type,first(episode) episode, first(area) area, first(year) year, first(videoLengthType) videoLengthType, " +
      "first(create_time) create_time, first(publish_time) publish_time, first(douban_id) douban_id, first(source) source, " +
      "first(language_code) language_code, first(supply_type) supply_type, first(tags) tags" +
      " from mtv_basecontent where sid is not null and sid <> '' and display_name is not null " +
      " group by sid ").registerTempTable("program_table")

    val contentTypeDb = MysqlDB.whaleyCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().registerTempTable("content_type")

    sqlContext.sql("SELECT a.sid, a.display_name as title, " +
      "a.content_type, b.name as content_type_name, a.duration, a.video_type, a.episode as episode_index, a.area, a.year, " +
      "a.videoLengthType as video_length_type, a.create_time, a.publish_time, a.douban_id, a.source, a.language_code, a.supply_type, a.tags " +
      " from program_table a left join content_type b on a.content_type = b.code " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id")
  }
}
