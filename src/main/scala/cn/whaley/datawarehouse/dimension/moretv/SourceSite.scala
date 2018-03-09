package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by Tony on 17/3/23.
  */
object SourceSite extends DimensionBase {
  columns.skName = "source_site_sk"
  columns.primaryKeys = List("source_site_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "source_site_id",
    "site_content_type",
    "main_category",
    "main_category_code",
    "second_category",
    "second_category_code",
    "third_category",
    "third_category_code",
    "fourth_category",
    "fourth_category_code")

  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_program_site", "id", 1, 500, 5)

  dimensionName = "dim_medusa_source_site"

  fullUpdate = true

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    sourceDf.filter("status = 1").filter("id <> 1").withColumn(
      //补充源数据中纪录片code缺失
      "code", expr("case when contentType = 'jilu' and name = '纪录片' then 'site_jilu' else code end")
    ).registerTempTable("mtv_program_site")

    val siteRootList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 409, 450, 471, 1831, 3081, 3241, 8461, 10161, 10591, 11075, 11154, 11155, 11429, 11613, 11627)
    //1-站点树、2-电影、3-电视剧、4-动漫、5-综艺、6-纪录片、7-资讯短片、8-音乐、9-戏曲、409-少儿站点树、450-音乐站点树、471-体育站点树、11613-粤语、11627-会员

    //最大包含四级目录，从包含4几目录的站点树开始
    val df4 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, a.contentType site_content_type, " +
      "d.name AS main_category, d.code AS main_category_code, " +
      "c.name AS second_category, c.code AS second_category_code, " +
      "b.name AS third_category, b.code AS third_category_code," +
      "a.name AS fourth_category, a.code AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id)" +
      " INNER JOIN mtv_program_site AS d ON ( c.parentId = d.id and d.status = 1) " +
      " WHERE d.id IN (" + siteRootList.mkString(",") + ")")

    val df3 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, a.contentType site_content_type, " +
      "c.name AS main_category, c.code AS main_category_code,  " +
      "b.name AS second_category, b.code AS second_category_code, " +
      "a.name AS third_category, a.code AS third_category_code," +
      "null AS fourth_category, null AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id )" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id and c.status = 1)" +
      " WHERE c.id IN (" + siteRootList.mkString(",") + ")")

    val df2 = sqlContext.sql("SELECT cast(a.id as long) source_site_id, " +
      "if(b.contentType in ('member','cantonese'),b.contentType,a.contentType) as site_content_type," + //319会员和粤语频道的站点树的contentType与所属频道不一致
      "b.name AS main_category, b.code AS main_category_code, " +
      "a.name AS second_category, a.code AS second_category_code, " +
      "null AS third_category, null AS third_category_code, " +
      "null AS fourth_category, null AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id and b.status = 1)" +
      " WHERE b.id IN (" + siteRootList.mkString(",") + ") ")

    df4.unionAll(df3).unionAll(df2).orderBy("source_site_id")
  }

}
