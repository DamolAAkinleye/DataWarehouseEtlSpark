package cn.whaley.datawarehouse.dimension.share

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 16/12/23.
  */
object AppVersion extends BaseClass {
  private val tableName = "moretv_app_version"
  private val dimensionType = "dim_app_version"
  override def execute(args: Array[String]): Unit = {
    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.medusaAppVersion).load()
    jdbcDF.registerTempTable("moretv_app_version")

    val df = sqlContext.sql("SELECT cast((id + 10000) as long) as app_version_key, app_name, app_en_name, " +
      "'' as app_id, app_series, version, build_time,  " +
      "'微鲸' as company, '' as product" +
      s" from $tableName")

   /* HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_app_version")
    df.write.parquet("/data_warehouse/dw_dimensions/dim_app_version")*/
    (df,dimensionType)
  }
}
