package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.dimension.constant.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/3/23.
  */
object SourceSite extends DimensionBase {
  columns.skName = "source_site_sk"
  columns.primaryKeys = List("source_site_id")
  columns.trackingColumns = List()
  columns.otherColumns = List("content_type", "main_category", "main_category_code", "second_category",
    "second_category_code", "third_category", "third_category_code", "fourth_category", "fourth_category_code")

  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_program_site", "id", 1, 500, 5)

  dimensionName = "dim_medusa_source_site"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    sourceDf.filter("status = 1").filter("id <> 1").registerTempTable("mtv_program_site")

    //最大包含四级目录，从包含4几目录的站点树开始
    val df4 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, a.contentType content_type, " +
      "d.name AS main_category, d.code AS main_category_code, " +
      "c.name AS second_category, c.code AS second_category_code, " +
      "b.name AS third_category, b.code AS third_category_code," +
      "a.name AS fourth_category, a.code AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id)" +
      " INNER JOIN mtv_program_site AS d ON ( c.parentId = d.id)" +
      " WHERE d.parentId IN (0, 1)")

    val df3 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, a.contentType content_type," +
      "c.name AS main_category, c.code AS main_category_code,  " +
      "b.name AS second_category, b.code AS second_category_code, " +
      "a.name AS third_category, a.code AS third_category_code," +
      "null AS fourth_category, null AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id )" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id )" +
      " WHERE c.parentId IN (0,1)")

    val df2 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, a.contentType AS content_type, " +
      "b.name AS main_category, b.code AS main_category_code, " +
      "a.name AS second_category, a.code AS second_category_code, " +
      "null AS third_category, null AS third_category_code, " +
      "null AS fourth_category, null AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " WHERE b.parentId IN (0,1)")

    df4.unionAll(df3).unionAll(df2).orderBy("source_site_id")
  }

}
