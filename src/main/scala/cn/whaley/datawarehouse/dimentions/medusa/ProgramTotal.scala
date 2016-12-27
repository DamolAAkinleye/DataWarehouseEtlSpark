package cn.whaley.datawarehouse.dimentions.medusa

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB}

/**
  * Created by Tony on 16/12/22.
  *
  * 电视猫节目信息全量更新
  */
object ProgramTotal extends BaseClass {
  override def execute(args: Array[String]): Unit = {

    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaCmsBaseContent).load()
    jdbcDF.registerTempTable("mtv_basecontent")

    val df = sqlContext.sql("SELECT a.id as program_sk, a.sid, a.display_name as title, " +
      "a.content_type, a.duration, a.video_type, a.episode as episode_index, " +
      "b.sid as episode_sid, a.area, a.year, a.videoLengthType as video_length_type, " +
      "a.create_time, " +
      "a.publish_time " +
      " from mtv_basecontent a left join mtv_basecontent b on a.parent_id = b.id " +
      " ORDER BY a.id")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dimensions/medusa/program")
    df.write.parquet("/data_warehouse/dimensions/medusa/program")
  }

}
