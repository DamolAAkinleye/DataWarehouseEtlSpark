package cn.whaley.datawarehouse.normalized.tag

import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB, Params}
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/4/19.
  */
object ProgramTag extends NormalizedEtlBase{

  tableName = "program_tag"

  override def extract(params: Params): DataFrame = {
    val sourceDb = MysqlDB.programTag("tag", "id", 1, 500000, 100)
    DataExtractUtils.readFromJdbc(sqlContext,sourceDb)
  }


  override def transform(params: Params, df: DataFrame): DataFrame = {
    df.where("status = 1").selectExpr("id","tag_name","tag_type_id","is_media_source","is_douban","is_blacklist","tag_type")
  }
}
