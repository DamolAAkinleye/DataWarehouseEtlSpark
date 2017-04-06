package cn.whaley.datawarehouse.dimension.moretv.total

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB, Params}

/**
  * Created by Tony on 16/12/22.
  *
  * 电视猫节目信息全量更新
  */
object ProgramTotal extends BaseClass {
  override def execute(params: Params): Unit = {

    val jdbcDF = sqlContext.read.format("jdbc")
      .options(MysqlDB.medusaCms("mtv_basecontent", "id",1, 2001314181, 100))
      .load()
    jdbcDF.registerTempTable("mtv_basecontent")

    //清洗数据
    sqlContext.sql("select sid, first(id) id, first(display_name) display_name, first(content_type) content_type, " +
      " first(duration) duration, first(parent_id) parent_id, first(video_type) video_type," +
      " first(episode) episode, first(area) area, first(year) year, " +
      " first(videoLengthType) videoLengthType, first(create_time) create_time, first(publish_time) publish_time " +
      " from mtv_basecontent where sid is not null and sid <> '' and display_name is not null " +
      " group by sid ").registerTempTable("program_table")

    val df = sqlContext.sql("SELECT cast(a.id as long) as program_sk, a.sid, a.display_name as title, " +
      "a.content_type, a.duration, a.video_type, a.episode as episode_index, " +
      "b.sid as parent_sid, a.area, a.year, a.videoLengthType as video_length_type, " +
      "a.create_time, " +
      "a.publish_time " +
      " from program_table a left join program_table b on a.parent_id = b.id " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_medusa_program")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_medusa_program")
  }

}
