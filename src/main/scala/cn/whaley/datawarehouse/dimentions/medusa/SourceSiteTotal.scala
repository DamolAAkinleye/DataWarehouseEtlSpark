package cn.whaley.datawarehouse.dimentions.medusa

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.{HdfsUtil, MysqlDB}

/**
  * Created by Tony on 17/1/12.
  */
object SourceSiteTotal extends BaseClass {
  override def execute(args: Array[String]): Unit = {

    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaCms("mtv_program_site", "id", 1, 500, 5)).load()
    jdbcDF.filter("status = 1").filter("id <> 1").registerTempTable("mtv_program_site")

    //最大包含四级目录，从包含4几目录的站点树开始
    val df4 = sqlContext.sql("SELECT cast(a.id as long) source_site_sk, a.contentType content_type, " +
      "d.name AS main_category, d.code AS main_category_code, " +
      "c.name AS second_category, c.code AS second_category_code, " +
      "b.name AS third_category, b.code AS third_category_code," +
      "a.name AS fourth_category, a.code AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id)" +
      " INNER JOIN mtv_program_site AS d ON ( c.parentId = d.id)" +
      " WHERE d.parentId IN (0, 1)")

    val df3 = sqlContext.sql("SELECT cast(a.id as long) source_site_sk, a.contentType content_type," +
      "c.name AS main_category, c.code AS main_category_code,  " +
      "b.name AS second_category, b.code AS second_category_code, " +
      "a.name AS third_category, a.code AS third_category_code," +
      "'' AS fourth_category, '' AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id )" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id )" +
      " WHERE c.parentId IN (0,1)")

    val df2 = sqlContext.sql("SELECT cast(a.id as long) source_site_sk, a.contentType AS content_type, " +
      "b.name AS main_category, b.code AS main_category_code, " +
      "a.name AS second_category, a.code AS second_category_code, " +
      "'' AS third_category, '' AS third_category_code, " +
      "'' AS fourth_category, '' AS fourth_category_code " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " WHERE b.parentId IN (0,1)")

    val df = df4.unionAll(df3).unionAll(df2).orderBy("source_site_sk")

    HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_medusa_source_site")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_medusa_source_site")
  }
}
