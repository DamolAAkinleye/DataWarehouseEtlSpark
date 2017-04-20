package cn.whaley.datawarehouse.normalized.tag

import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB, Params}
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/4/19.
  */
object ProgramTagMapping extends NormalizedEtlBase {

  tableName = "program_tag_mapping"

  override def extract(params: Params): DataFrame = {
    val sourceDb = MysqlDB.programTag("tag_program_mapping", "id", 1, 5000000, 200)
    val programTagDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb).where("status = 1")

    val sourceDb2 = MysqlDB.programTag("tag_mapping", "id", 1, 1000, 10)
    val tagMappingDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb2).where("status = 1")

    val sourceDb3 = MysqlDB.programTag("tag_program", "id", 1, 2000000, 200)
    val programDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb3).where("status = 1")

    val sourceDb4 = MysqlDB.programTag("tag", "id", 1, 500000, 100)
    val tagDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb4).where("status = 1")

    val df = programTagDf.join(programDf, List("sid"), "leftouter").selectExpr(
      "sid", "title", "tag_id", "tag_level_value", "type", "content_type", "program_status").as("a").join(
      tagMappingDf.as("b"), programTagDf("tag_id") === tagMappingDf("tag_id"), "leftouter"
    ).selectExpr(
      "sid",
      "title",
      "case when b.mapping_tag_id is null then a.tag_id else b.mapping_tag_id end as tag_id",
      "tag_level_value",
      "type",
      "content_type",
      "program_status"
    )
    df.as("p").join(
      tagDf.as("t"), df("tag_id") === tagDf("id"), "leftouter"
    ).selectExpr(
      "sid as program_sid",
      "title as program_title",
      "tag_id",
      "t.tag_name",
      "tag_level_value",
      "type as program_type",
      "content_type",
      "program_status"
    )
  }


  override def transform(params: Params, df: DataFrame): DataFrame = {
    df
  }
}
